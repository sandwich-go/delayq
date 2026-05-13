package delayq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// moveLua 把 source ZSET 中 score <= max_priority 的成员搬到 target，
// 同时把 target 中的 score 设为 to_score。返回原始的 [value, score, value, score, ...]
var moveLua = `
local source_set, target_set  = KEYS[1], KEYS[2]
local max_priority, to_score = ARGV[1], ARGV[2]
local items = redis.call('ZRANGEBYSCORE', source_set, '-inf', max_priority, 'WITHSCORES')
for i, value in ipairs(items) do
	if i % 2 ~= 0 then
		redis.call('ZADD', target_set, to_score or 0.0, value)
		redis.call('ZREM', source_set, value)
	end
end
return items
`

// lengthLua 返回 [delay 集长度, doing 集长度]
var lengthLua = `
local delay_set, doing_set  = KEYS[1], KEYS[2]
local l1 = redis.call('ZCARD', delay_set)
local l2 = redis.call('ZCARD', doing_set)
return {l1, l2}
`

// addLua 把若干 (value, score) 对添加到 delay 集。
// ARGV: value1, score1, value2, score2, ...
var addLua = `
local delay_set  = KEYS[1]
for i = 1, #ARGV, 2 do
	local v, s = ARGV[i], ARGV[i+1]
	redis.call('ZADD', delay_set, s, v)
end
return {true}
`

// ackSuccessLua 业务处理成功，从 delay/doing/failed 三处清除
var ackSuccessLua = `
local delay_set, doing_set, failed_hash  = KEYS[1], KEYS[2], KEYS[3]
local value = ARGV[1]
redis.call('ZREM', delay_set, value)
redis.call('ZREM', doing_set, value)
redis.call('HDEL', failed_hash, value)
return {true}
`

// ackFailedLua 业务处理失败：
// - 从 doing 移除
// - 重新加入 delay，score=next_score（未来时间戳）
// - 失败计数 Hash[value] += 1，返回新的失败计数
var ackFailedLua = `
local delay_set, doing_set, failed_hash  = KEYS[1], KEYS[2], KEYS[3]
local value, next_score = ARGV[1], ARGV[2]
redis.call('ZREM', doing_set, value)
redis.call('ZADD', delay_set, next_score, value)
local cnt = redis.call('HINCRBY', failed_hash, value, 1)
return {cnt}
`

// failedCountLua 读取多个 value 的失败计数
var failedCountLua = `
local failed_hash = KEYS[1]
local out = {}
for i, v in ipairs(ARGV) do
	local c = redis.call('HGET', failed_hash, v)
	if c == false then c = 0 end
	out[i] = c
end
return out
`

// getLua 查询 value 在 delay/doing 集中的状态与 score。
// 返回 {state, score} state: 0=不存在 1=delay 2=doing
var getLua = `
local delay_set, doing_set = KEYS[1], KEYS[2]
local value = ARGV[1]
local s1 = redis.call('ZSCORE', delay_set, value)
if s1 then return {1, s1} end
local s2 = redis.call('ZSCORE', doing_set, value)
if s2 then return {2, s2} end
return {0, 0}
`

// cancelLua 从 delay/doing/failed 三处删除 value，返回删除数量
var cancelLua = `
local delay_set, doing_set, failed_hash = KEYS[1], KEYS[2], KEYS[3]
local value = ARGV[1]
local n = 0
n = n + redis.call('ZREM', delay_set, value)
n = n + redis.call('ZREM', doing_set, value)
redis.call('HDEL', failed_hash, value)
return {n}
`

// RedisScript redis 脚本
type RedisScript interface {
	EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)
	Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)
}

// RedisScriptBuilder redis 脚本工厂，可以通过 src load
type RedisScriptBuilder interface {
	Build(src string) RedisScript
}

// priorityScale 优先级在 score 中占的最大权重（秒级）。
// score = execTimestamp - priority * priorityScale；priorityScale=1e-6 表示 priority 在 microsecond
// 级别影响排序，不会跨秒错位（10^9 时间戳 + 10^-6 weight 仍在 double 精度内）。
const priorityScale = 1e-6

// itemScore 计算 item 的 ZSET score
func itemScore(execTs int64, priority int32) float64 {
	return float64(execTs) - float64(priority)*priorityScale
}

// scoreToExecTs 从 score 还原原始执行时间戳（向上取整避免边界 off-by-one）
func scoreToExecTs(score float64) int64 {
	// score 可能为浮点，结果四舍五入到秒
	if score >= 0 {
		return int64(score + 0.5)
	}
	return int64(score - 0.5)
}

type redisQueue struct {
	*baseQueue

	delaySetKey   string
	doingSetKey   string
	failedHashKey string

	moveScript        RedisScript
	addScript         RedisScript
	lengthScript      RedisScript
	ackSuccessScript  RedisScript
	ackFailedScript   RedisScript
	failedCountScript RedisScript
	getScript         RedisScript
	cancelScript      RedisScript
}

// NewRedisTopicQueue 构造一个使用 Redis 作为后端的 TopicQueue。
// 必须通过 WithRedisScriptBuilder 注入 Redis 客户端适配器，否则会 panic。
func NewRedisTopicQueue(ctx context.Context, topic string, opts ...Option) TopicQueue {
	return newRedisTopicQueue(ctx, topic, newConfig(opts...))
}

func newRedisTopicQueue(ctx context.Context, topic string, opts *Options) TopicQueue {
	builder := opts.GetRedisScriptBuilder()
	q := &redisQueue{
		delaySetKey:       fmt.Sprintf("do:{%s}", topic),
		doingSetKey:       fmt.Sprintf("doing:{%s}", topic),
		failedHashKey:     fmt.Sprintf("failed:{%s}", topic),
		moveScript:        builder.Build(moveLua),
		addScript:         builder.Build(addLua),
		lengthScript:      builder.Build(lengthLua),
		ackSuccessScript:  builder.Build(ackSuccessLua),
		ackFailedScript:   builder.Build(ackFailedLua),
		failedCountScript: builder.Build(failedCountLua),
		getScript:         builder.Build(getLua),
		cancelScript:      builder.Build(cancelLua),
	}
	if prefix := opts.GetPrefix(); len(prefix) > 0 {
		q.delaySetKey = fmt.Sprintf("%s:%s", prefix, q.delaySetKey)
		q.doingSetKey = fmt.Sprintf("%s:%s", prefix, q.doingSetKey)
		q.failedHashKey = fmt.Sprintf("%s:%s", prefix, q.failedHashKey)
	}
	q.baseQueue = newBaseQueue(ctx, topic, opts)
	q.success = q.onSuccess
	q.failed = q.onFailed
	return q
}

// opCtx 返回基于 q.ctx 的操作上下文，保证 Close/ctx 取消时 Redis 调用可被及时中断
func (q *redisQueue) opCtx() context.Context {
	if q.ctx != nil {
		return q.ctx
	}
	return context.Background()
}

// Push 将 item 加入延迟队列，DelaySecond 为相对秒数；item.Priority 用于同时间内排序
func (q *redisQueue) Push(item *Item) error {
	if err := q.prepareItem(item); err != nil {
		return err
	}
	delay := item.GetDelaySecond()
	if delay < 0 {
		delay = 0
	}
	score := itemScore(unix()+delay, item.GetPriority())
	_, err := q.runScript(q.opCtx(), q.addScript, []string{q.delaySetKey}, item.GetValue(), score)
	return err
}

// PushBatch 批量推送，原子地通过单个 Lua 脚本完成所有 ZADD
func (q *redisQueue) PushBatch(items []*Item) error {
	if len(items) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(items)*2)
	now := unix()
	for _, it := range items {
		if err := q.prepareItem(it); err != nil {
			return err
		}
		delay := it.GetDelaySecond()
		if delay < 0 {
			delay = 0
		}
		args = append(args, it.GetValue(), itemScore(now+delay, it.GetPriority()))
	}
	_, err := q.runScript(q.opCtx(), q.addScript, []string{q.delaySetKey}, args...)
	return err
}

// Length 返回 delay 集中等待执行的 item 数
func (q *redisQueue) Length() int64 {
	res, err := q.runScript(q.opCtx(), q.lengthScript, []string{q.delaySetKey, q.doingSetKey})
	if err != nil {
		q.log.Errorf("topic=%s length error: %v", q.topic, err)
		return 0
	}
	if len(res) == 0 {
		return 0
	}
	v, _ := res[0].(int64)
	return v
}

// Get 查询 value 是否存在于队列中（delay 或 doing），返回剩余延迟（doing 中返回 0）
func (q *redisQueue) Get(value []byte) (remaining time.Duration, exists bool, err error) {
	res, err := q.runScript(q.opCtx(), q.getScript, []string{q.delaySetKey, q.doingSetKey}, value)
	if err != nil {
		return 0, false, err
	}
	if len(res) < 2 {
		return 0, false, nil
	}
	state := parseInt64(res[0])
	if state == 0 {
		return 0, false, nil
	}
	score := parseFloat64(res[1])
	if state == 2 {
		return 0, true, nil
	}
	execTs := scoreToExecTs(score)
	now := unix()
	if execTs <= now {
		return 0, true, nil
	}
	return time.Duration(execTs-now) * time.Second, true, nil
}

// Cancel 从 delay/doing 集与 failed Hash 中移除 value，返回是否移除成功
func (q *redisQueue) Cancel(value []byte) (bool, error) {
	res, err := q.runScript(q.opCtx(), q.cancelScript,
		[]string{q.delaySetKey, q.doingSetKey, q.failedHashKey},
		value)
	if err != nil {
		return false, err
	}
	if len(res) == 0 {
		return false, nil
	}
	return parseInt64(res[0]) > 0, nil
}

func (q *redisQueue) Close() error { return q.close() }

func (q *redisQueue) Start(f func(item *Item) error) error {
	return q.start(f, ticker{d: 1 * time.Second, f: q.poll}, ticker{d: 1 * time.Second, f: q.reclaim})
}

// StartManualAck 启动手动 ack 模式
func (q *redisQueue) StartManualAck(f func(item *Item, ack Acker)) error {
	q.manualHandler = f
	return q.start(func(*Item) error { return nil },
		ticker{d: 1 * time.Second, f: q.poll},
		ticker{d: 1 * time.Second, f: q.reclaim})
}

func (q *redisQueue) runScript(ctx context.Context, s RedisScript, keys []string, args ...interface{}) ([]interface{}, error) {
	ret, err := s.EvalSha(ctx, keys, args...)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		ret, err = s.Eval(ctx, keys, args...)
	}
	return ret, err
}

// move 把 source 中 score<=now 的项搬到 target，target 的 score 设为 toScore
func (q *redisQueue) move(from, to string, maxPriority, toScore int64) ([]interface{}, error) {
	return q.runScript(q.opCtx(), q.moveScript, []string{from, to}, maxPriority, toScore)
}

// poll 把 delay 集中到期的 item 搬到 doing 集，doing 集 score 设为 now+VisibilityTimeout，
// 然后批量派发给业务 handler
func (q *redisQueue) poll() error {
	now := unix()
	visTimeout := int64(q.opts.GetVisibilityTimeout() / time.Second)
	if visTimeout <= 0 {
		visTimeout = 1
	}
	res, err := q.move(q.delaySetKey, q.doingSetKey, now, now+visTimeout)
	if err != nil {
		q.monitorCount(MetricPollError)
		return err
	}
	// 收集 values 用于批量查询失败计数
	type pending struct {
		item  *Item
		value string
	}
	var pendings []pending
	for i := 0; i+1 < len(res); i += 2 {
		val, ok := res[i].(string)
		if !ok {
			continue
		}
		pendings = append(pendings, pending{
			item:  &Item{Value: []byte(val)},
			value: val,
		})
	}
	if len(pendings) == 0 {
		return nil
	}
	// 查失败计数
	values := make([]interface{}, len(pendings))
	for i, p := range pendings {
		values[i] = p.value
	}
	counts, cerr := q.runScript(q.opCtx(), q.failedCountScript, []string{q.failedHashKey}, values...)
	if cerr != nil {
		q.log.Errorf("topic=%s failed count query error: %v", q.topic, cerr)
		// 即使查询失败也要派发，按 0 失败计数处理
		counts = make([]interface{}, len(pendings))
	}
	for i, p := range pendings {
		var failed int64
		if i < len(counts) {
			failed = parseInt64(counts[i])
		}
		// item.DelaySecond 编码失败次数（负值），便于 onFailed 正确累加
		if failed > 0 {
			p.item.DelaySecond = -failed
		}
		// 已达重试上限，直接死信
		if int(failed) >= q.opts.GetRetryTimes() && q.opts.GetRetryTimes() > 0 {
			q.invokeDeadLetter(p.item)
			if aerr := q.onSuccess(p.item); aerr != nil {
				q.log.Errorf("topic=%s ack dead letter error: %v", q.topic, aerr)
			}
			continue
		}
		q.execute(p.item)
	}
	return nil
}

// reclaim 把 doing 集中已过 visibility 的 item 搬回 delay 集（重新等待 poll）
func (q *redisQueue) reclaim() error {
	now := unix()
	items, err := q.move(q.doingSetKey, q.delaySetKey, now, now)
	if err != nil {
		q.monitorCount(MetricReclaimError)
	} else {
		q.monitorCount(MetricReclaim, len(items)/2)
	}
	return err
}

// onFailed 业务处理失败：从 doing 删除，重新加入 delay，并累加失败计数
// 重试间隔由 computeRetryDelay 决定
func (q *redisQueue) onFailed(item *Item) error {
	currentFailed := 0
	if item.GetDelaySecond() < 0 {
		currentFailed = int(-item.GetDelaySecond())
	}
	nextFailed := currentFailed + 1
	delay := computeRetryDelay(q.opts, nextFailed)
	delaySec := int64(delay / time.Second)
	if delaySec < 0 {
		delaySec = 0
	}
	nextScore := itemScore(unix()+delaySec, item.GetPriority())
	_, err := q.runScript(q.opCtx(), q.ackFailedScript,
		[]string{q.delaySetKey, q.doingSetKey, q.failedHashKey},
		item.GetValue(), nextScore)
	return err
}

// onSuccess 业务处理成功：清除 doing 与失败计数
func (q *redisQueue) onSuccess(item *Item) error {
	_, err := q.runScript(q.opCtx(), q.ackSuccessScript,
		[]string{q.delaySetKey, q.doingSetKey, q.failedHashKey},
		item.GetValue())
	return err
}

// parseInt64 兼容 Redis 返回的 int64/string
func parseInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// parseFloat64 兼容 Redis 返回的多种数字类型
func parseFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

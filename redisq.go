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

// addLua 把 value 添加到 delay 集，score 表示该 item 的预期执行时间戳
var addLua = `
local delay_set  = KEYS[1]
local value, score = ARGV[1], ARGV[2]
redis.call('ZADD', delay_set, score or 0.0, value)
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

// RedisScript redis 脚本
type RedisScript interface {
	EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)
	Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)
}

// RedisScriptBuilder redis 脚本工厂，可以通过 src load
type RedisScriptBuilder interface {
	Build(src string) RedisScript
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
}

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

// Push 将 item 加入延迟队列，DelaySecond 为相对秒数
func (q *redisQueue) Push(item *Item) error {
	if err := q.prepareItem(item); err != nil {
		return err
	}
	score := unix() + item.GetDelaySecond()
	if item.GetDelaySecond() < 0 {
		score = unix()
	}
	_, err := q.runScript(q.opCtx(), q.addScript, []string{q.delaySetKey}, item.GetValue(), score)
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

func (q *redisQueue) Close() error { return q.close() }

func (q *redisQueue) Start(f func(item *Item) error) error {
	return q.start(f, ticker{d: 1 * time.Second, f: q.poll}, ticker{d: 1 * time.Second, f: q.reclaim})
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
		q.monitorCount("delayq_poll_error")
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
		q.monitorCount("delayq_reclaim_error")
	} else {
		q.monitorCount("delayq_reclaim", len(items)/2)
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
	nextScore := unix() + delaySec
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

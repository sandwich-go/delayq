package delayq

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const safeSec = 10 * 60 // 拖底的时间

var moveLua = `
local source_set, target_set  = KEYS[1], KEYS[2]
local max_priority, score = ARGV[1], ARGV[2]
local items = redis.call('ZRANGEBYSCORE', source_set, '-inf', max_priority, 'WITHSCORES')
for i, value in ipairs(items) do
	if i % 2 ~= 0 then
		redis.call('ZADD', target_set, score or 0.0, value)
		redis.call('ZREM', source_set, value)
	end
end
return items
`

var lengthLua = `
local delay_set, doing_set  = KEYS[1], KEYS[2]
local l1 = redis.call('ZCARD', delay_set)
local l2 = redis.call('ZCARD', doing_set)
return {l1, l2}
`

var addLua = `
local delay_set  = KEYS[1]
local value, score = ARGV[1], ARGV[2]
redis.call('ZADD', delay_set, score or 0.0, value)
return {true}
`

var ackSuccessLua = `
local delay_set, doing_set  = KEYS[1], KEYS[2]
local value = ARGV[1]
redis.call('ZREM', delay_set, value)
redis.call('ZREM', doing_set, value)
return {true}
`

var ackFailedLua = `
local delay_set, doing_set  = KEYS[1], KEYS[2]
local value, score = ARGV[1], ARGV[2]
redis.call('ZREM', doing_set, value)
redis.call('ZADD', delay_set, score-1, value)
return {true}
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
	baseQueue

	delaySetKey string
	doingSetKey string

	moveScript       RedisScript
	addScript        RedisScript
	lengthScript     RedisScript
	ackSuccessScript RedisScript
	ackFailedScript  RedisScript
}

func NewRedisTopicQueue(ctx context.Context, topic string, opts ...Option) TopicQueue {
	return newRedisTopicQueue(ctx, topic, newConfig(opts...))
}

func newRedisTopicQueue(ctx context.Context, topic string, opts *Options) TopicQueue {
	builder := opts.GetRedisScriptBuilder()
	q := &redisQueue{
		delaySetKey:      fmt.Sprintf("do:{%s}", topic),
		doingSetKey:      fmt.Sprintf("doing:{%s}", topic),
		moveScript:       builder.Build(moveLua),
		addScript:        builder.Build(addLua),
		lengthScript:     builder.Build(lengthLua),
		ackSuccessScript: builder.Build(ackSuccessLua),
		ackFailedScript:  builder.Build(ackFailedLua),
	}
	if prefix := opts.GetPrefix(); len(prefix) > 0 {
		q.delaySetKey = fmt.Sprintf("%s:%s", prefix, q.delaySetKey)
		q.doingSetKey = fmt.Sprintf("%s:%s", prefix, q.doingSetKey)
	}
	q.baseQueue = baseQueue{ctx: ctx, opts: opts, topic: topic, success: q.onSuccess, failed: q.onFailed}
	return q
}

func (q *redisQueue) Push(item *Item) error {
	now := unix()
	_, err := q.runScript(context.Background(), q.addScript, []string{q.delaySetKey}, item.GetValue(), now)
	return err
}

func (q *redisQueue) Length() int64 {
	res, err := q.runScript(context.Background(), q.lengthScript, []string{q.delaySetKey, q.doingSetKey})
	if err != nil {
		fmt.Println("length error", err)
		return 0
	}
	return res[0].(int64)
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

func (q *redisQueue) move(from, to string, offset float64) ([]interface{}, error) {
	now := unix()
	return q.runScript(context.Background(), q.moveScript, []string{from, to}, now, float64(now)+offset)
}

func (q *redisQueue) poll() error {
	res, err := q.move(q.delaySetKey, q.doingSetKey, safeSec)
	if err != nil {
		q.monitorCount("delayq_poll_error", 1, map[string]string{"Queue": q.topic})
		return err
	}
	for i := 0; i < len(res); i += 2 {
		item := &Item{Value: []byte(res[i].(string))}
		score, _ := strconv.ParseInt(res[i+1].(string), 10, 64)
		if score < 0 {
			// 曾经失败过
			item.DelaySecond = score
			// 如果失败次数太多，抛给业务
			if int(math.Abs(float64(score))) >= q.opts.GetRetryTimes() {
				if f := q.opts.GetOnDeadLetter(); f != nil {
					f(item)
				}
				_ = q.onSuccess(item)
				continue
			}
		}
		q.execute(item)
	}
	return nil
}

func (q *redisQueue) reclaim() error {
	items, err := q.move(q.doingSetKey, q.delaySetKey, 0)
	if err != nil {
		q.monitorCount("delayq_reclaim_error", 1, map[string]string{"Queue": q.topic})
	} else {
		q.monitorCount("delayq_reclaim", int64(len(items)/2), map[string]string{"Queue": q.topic})
	}
	return err
}

func (q *redisQueue) onFailed(item *Item) error {
	_, err := q.runScript(context.Background(), q.ackFailedScript, []string{q.delaySetKey, q.doingSetKey}, item.GetValue(), item.GetDelaySecond())
	return err
}

func (q *redisQueue) onSuccess(item *Item) error {
	_, err := q.runScript(context.Background(), q.ackSuccessScript, []string{q.delaySetKey, q.doingSetKey}, item.GetValue(), 0)
	return err
}

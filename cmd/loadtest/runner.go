package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandwich-go/delayq"
	"github.com/sandwich-go/redisson"
)

// startProducers 启动 N 个生产者，按目标 QPS 推送，返回等待生产者退出的函数。
func startProducers(ctx context.Context, cfg *config, dq delayq.Queue, stats *stats) func() {
	perProducerQPS := cfg.qps / cfg.producers
	if perProducerQPS < 1 {
		perProducerQPS = 1
	}
	interval := time.Second / time.Duration(perProducerQPS)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	value := make([]byte, cfg.maxValueLen)
	for i := range value {
		value[i] = 'x'
	}

	var wg sync.WaitGroup
	var seq int64
	for p := 0; p < cfg.producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(pid)))
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				id := atomic.AddInt64(&seq, 1)
				delay := int64(r.Intn(int(cfg.maxDelay/time.Second) + 1))
				// 复制 value 并替换前 N 字节为唯一 id（Redis 模式下 value 不可重复）
				v := make([]byte, len(value))
				copy(v, value)
				idStr := strconv.FormatInt(id, 10)
				if len(idStr) > len(v) {
					v = []byte(idStr)
				} else {
					copy(v, idStr)
				}
				start := time.Now()
				err := dq.Push(&delayq.Item{
					Topic:       cfg.topic,
					DelaySecond: delay,
					Value:       v,
				})
				stats.observePushLatency(time.Since(start))
				if err != nil {
					stats.incPushError()
				} else {
					stats.incPushOK()
					// 记录 push 时间用于 e2e 延迟统计
					stats.recordPushTime(string(v[:min(len(v), 16)]), time.Now().Add(time.Duration(delay)*time.Second))
				}
			}
		}(p)
	}
	return wg.Wait
}

// makeHandler 构造业务 handler，按配置注入 panic / error / 处理耗时
func makeHandler(cfg *config, stats *stats) func(*delayq.Item) error {
	return func(item *delayq.Item) error {
		start := time.Now()
		// 模拟处理耗时
		if cfg.handleTime > 0 {
			time.Sleep(cfg.handleTime)
		}

		// 计算 e2e 延迟（从 push expectedExecAt 到 handler 实际执行时间）
		key := string(item.GetValue()[:min(len(item.GetValue()), 16)])
		if expected, ok := stats.getPushTime(key); ok {
			latency := time.Since(expected)
			stats.observeE2ELatency(latency)
			stats.deletePushTime(key)
		}
		stats.observeHandleLatency(time.Since(start))

		// 注入 panic
		r := rand.Float64()
		if cfg.panicRate > 0 && r < cfg.panicRate {
			stats.incHandlePanic()
			panic("loadtest injected panic")
		}
		// 注入 error
		if cfg.errorRate > 0 && r < cfg.errorRate+cfg.panicRate {
			stats.incHandleError()
			return fmt.Errorf("loadtest injected error")
		}
		stats.incHandleOK()
		return nil
	}
}

// newRedisBuilder 构造 redisson 适配的 RedisScriptBuilder。
// MustNewClient 在不可达时直接 panic，因此当前不会真正返回错误，
// 但保留 error 返回值以便未来切换为 NewClient 形式。
func newRedisBuilder(addr string) delayq.RedisScriptBuilder {
	c := redisson.MustNewClient(redisson.NewConf(
		redisson.WithAddrs(addr),
		redisson.WithDevelopment(false),
	))
	return &redissonScriptBuilder{c: c}
}

type redissonScriptBuilder struct{ c redisson.Cmdable }

func (b *redissonScriptBuilder) Build(src string) delayq.RedisScript {
	return redissonScript{s: b.c.CreateScript(src)}
}

type redissonScript struct{ s redisson.Scripter }

func (s redissonScript) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.EvalSha(ctx, keys, args...).Slice()
}
func (s redissonScript) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.Eval(ctx, keys, args...).Slice()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

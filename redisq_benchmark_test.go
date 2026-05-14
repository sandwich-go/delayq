//go:build integration

package delayq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/sandwich-go/redisson"
)

// realRedisBuilderForBench 与 realRedisBuilder 类似，但接受 *testing.B
func realRedisBuilderForBench(b *testing.B) RedisScriptBuilder {
	b.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR not set")
	}
	c := redisson.MustNewClient(redisson.NewConf(
		redisson.WithAddrs(addr),
		redisson.WithDevelopment(false),
	))
	return redissonScriptBuilder{c: c}
}

// BenchmarkRedisQueue_Push 单线程 Redis Push
func BenchmarkRedisQueue_Push(b *testing.B) {
	topic := fmt.Sprintf("bench-redis-push-%d", b.N)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilderForBench(b)),
		WithLogger(NopLogger()),
	)
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := tp.Push(&Item{
			DelaySecond: 3600,
			Value:       []byte(strconv.Itoa(i)),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRedisQueue_Push_Parallel 多 goroutine 并发 Push
func BenchmarkRedisQueue_Push_Parallel(b *testing.B) {
	topic := fmt.Sprintf("bench-redis-push-p-%d", b.N)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilderForBench(b)),
		WithLogger(NopLogger()),
	)
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()

	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&counter, 1)
			if err := tp.Push(&Item{
				DelaySecond: 3600,
				Value:       []byte(strconv.FormatInt(id, 10)),
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisQueue_PushBatch 批量 push
func BenchmarkRedisQueue_PushBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		size := size
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			topic := fmt.Sprintf("bench-redis-batch-%d-%d", size, b.N)
			tp := NewRedisTopicQueue(context.Background(), topic,
				WithRedisScriptBuilder(realRedisBuilderForBench(b)),
				WithLogger(NopLogger()),
			)
			if err := tp.Start(noopHandler); err != nil {
				b.Fatal(err)
			}
			defer tp.Close()

			b.ResetTimer()
			b.ReportAllocs()
			seq := 0
			for i := 0; i < b.N; i++ {
				items := make([]*Item, size)
				for j := range items {
					seq++
					items[j] = &Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(seq))}
				}
				if err := tp.PushBatch(items); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "items/s")
		})
	}
}

// BenchmarkRedisQueue_Length Length 调用开销
func BenchmarkRedisQueue_Length(b *testing.B) {
	topic := fmt.Sprintf("bench-redis-len-%d", b.N)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilderForBench(b)),
		WithLogger(NopLogger()),
	)
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	for i := 0; i < 1000; i++ {
		_ = tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))})
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tp.Length()
	}
}

// BenchmarkRedisQueue_Get
func BenchmarkRedisQueue_Get(b *testing.B) {
	topic := fmt.Sprintf("bench-redis-get-%d", b.N)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilderForBench(b)),
		WithLogger(NopLogger()),
	)
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	for i := 0; i < 1000; i++ {
		_ = tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))})
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = tp.Get([]byte(strconv.Itoa(i % 1000)))
	}
}



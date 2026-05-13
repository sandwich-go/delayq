package delayq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 全部 benchmark 都通过足够长的 delay (3600s) 避免 ticker 消费 item，
// 同时必须 Start 让 isClosed() 返回 false，否则 Push/PushBatch 会被短路成 ErrTopicQueueHasClosed
// 测不到真实路径。

// noopHandler 用作 benchmark 中 Start 的占位 handler
func noopHandler(*Item) error { return nil }

// ===== Push 性能 =====

// BenchmarkMemq_Push 单线程 Push 吞吐（每次 unique value，反映真实生产负载）
func BenchmarkMemq_Push(b *testing.B) {
	for _, idx := range []struct {
		name string
		opts []Option
	}{
		{"with-index", []Option{WithLogger(NopLogger())}},
		{"no-index", []Option{WithLogger(NopLogger()), WithDisableValueIndex(true)}},
	} {
		idx := idx
		b.Run(idx.name, func(b *testing.B) {
			tp := NewMemoryTopicQueue(context.Background(), "bench-push", idx.opts...)
			if err := tp.Start(noopHandler); err != nil {
				b.Fatal(err)
			}
			defer tp.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMemq_Push_Parallel 多线程 Push 吞吐（测锁争用）
func BenchmarkMemq_Push_Parallel(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-push-p", WithLogger(NopLogger()))
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
			if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.FormatInt(id, 10))}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemq_PushBatch 批量推送 vs 单条推送，每条 unique value
func BenchmarkMemq_PushBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		size := size
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			tp := NewMemoryTopicQueue(context.Background(), "bench-batch", WithLogger(NopLogger()))
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

// ===== handler 处理吞吐 =====

// BenchmarkMemq_HandlerThroughput 把 b.N 个 item 全部放到当前槽位然后触发 ticker，
// 测量 handler 调度开销。由于时间轮粒度 1s，避免 b.N 次 push 都受 ticker 抢锁干扰，
// 这里直接通过 insertLocked 旁路插入。
func BenchmarkMemq_HandlerThroughput(b *testing.B) {
	for _, conc := range []int{1, 16, 256} {
		conc := conc
		b.Run(fmt.Sprintf("conc=%d", conc), func(b *testing.B) {
			var done int64
			var wg sync.WaitGroup
			tp := NewMemoryTopicQueue(context.Background(), "bench-handler",
				WithLogger(NopLogger()),
				WithMaxConcurrency(conc),
				WithDisableValueIndex(true),
			)
			defer tp.Close()
			mq := tp.(*memQueue)
			if err := tp.Start(func(*Item) error {
				atomic.AddInt64(&done, 1)
				wg.Done()
				return nil
			}); err != nil {
				b.Fatal(err)
			}

			// 一次性把 b.N 个 item 直接插入当前槽位（bypass push）
			wg.Add(b.N)
			mq.mx.Lock()
			for i := 0; i < b.N; i++ {
				mq.insertLocked(&Item{Topic: "bench-handler"}, 0)
			}
			mq.mx.Unlock()

			b.ResetTimer()
			b.ReportAllocs()
			_ = mq.ticker()
			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(atomic.LoadInt64(&done))/b.Elapsed().Seconds(), "handles/s")
		})
	}
}

// ===== Get / Cancel =====

// BenchmarkMemq_Get 查询性能
func BenchmarkMemq_Get(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-get", WithLogger(NopLogger()))
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	for i := 0; i < 10000; i++ {
		if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = tp.Get([]byte(strconv.Itoa(i % 10000)))
	}
}

// BenchmarkMemq_Cancel 取消性能
func BenchmarkMemq_Cancel(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-cancel", WithLogger(NopLogger()))
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	for i := 0; i < b.N; i++ {
		if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tp.Cancel([]byte(strconv.Itoa(i)))
	}
}

// ===== Length / Status =====

// BenchmarkMemq_Length Length 调用本身的开销
func BenchmarkMemq_Length(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-length", WithLogger(NopLogger()))
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	for i := 0; i < 1000; i++ {
		if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte(strconv.Itoa(i))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tp.Length()
	}
}

// BenchmarkQueue_Status 多 topic 状态汇总开销
func BenchmarkQueue_Status(b *testing.B) {
	q := New(WithLogger(NopLogger()))
	defer q.Close()
	for i := 0; i < 32; i++ {
		topic := fmt.Sprintf("t%d", i)
		_ = q.Start(topic, func(item *Item) error { return nil })
		for j := 0; j < 100; j++ {
			_ = q.Push(&Item{Topic: topic, DelaySecond: 3600, Value: []byte(strconv.Itoa(j))})
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.Status()
	}
}

// ===== Wheel ticker =====

// BenchmarkMemq_TickerSweep 时间轮 tick 处理 N 个到期 item 的开销
func BenchmarkMemq_TickerSweep(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		n := n
		b.Run(fmt.Sprintf("due=%d", n), func(b *testing.B) {
			b.StopTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tp := NewMemoryTopicQueue(context.Background(), "bench-sweep",
					WithLogger(NopLogger()), WithMaxConcurrency(0))
				mq := tp.(*memQueue)
				_ = tp.Start(func(item *Item) error { return nil })
				// 把 n 个 item 全部放到当前槽位
				mq.mx.Lock()
				for j := 0; j < n; j++ {
					mq.insertLocked(&Item{Topic: "bench-sweep", Value: []byte(strconv.Itoa(j))}, 0)
				}
				mq.mx.Unlock()

				b.StartTimer()
				_ = mq.ticker()
				b.StopTimer()

				_ = tp.Close()
			}
		})
	}
}

// ===== 优先级排序 =====

// BenchmarkMemq_PushWithPriority 包含 priority 排序的 Push
func BenchmarkMemq_PushWithPriority(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-prio", WithLogger(NopLogger()))
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
			Priority:    int32(i % 100),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// ===== Manual ack 路径 =====

// BenchmarkMemq_ManualAck 测量 manual ack 派发链路（bypass push 避免 1s tick 抢锁）
func BenchmarkMemq_ManualAck(b *testing.B) {
	var wg sync.WaitGroup
	tp := NewMemoryTopicQueue(context.Background(), "bench-manual",
		WithLogger(NopLogger()), WithMaxConcurrency(256), WithDisableValueIndex(true))
	defer tp.Close()
	mq := tp.(*memQueue)
	if err := tp.StartManualAck(func(item *Item, ack Acker) {
		ack.Ack()
		wg.Done()
	}); err != nil {
		b.Fatal(err)
	}

	wg.Add(b.N)
	mq.mx.Lock()
	for i := 0; i < b.N; i++ {
		mq.insertLocked(&Item{Topic: "bench-manual"}, 0)
	}
	mq.mx.Unlock()

	b.ResetTimer()
	b.ReportAllocs()
	_ = mq.ticker()
	wg.Wait()
	b.StopTimer()
}

// ===== Allocations sanity =====

// BenchmarkMemq_Push_Alloc 显示单次 Push 的内存分配（zero-alloc 路径目标）
func BenchmarkMemq_Push_Alloc(b *testing.B) {
	tp := NewMemoryTopicQueue(context.Background(), "bench-alloc", WithLogger(NopLogger()))
	if err := tp.Start(noopHandler); err != nil {
		b.Fatal(err)
	}
	defer tp.Close()
	item := &Item{DelaySecond: 3600, Value: []byte("v")}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := tp.Push(item); err != nil {
			b.Fatal(err)
		}
	}
}

// ===== compute 工具函数 =====

func BenchmarkComputeRetryDelay(b *testing.B) {
	opts := newConfig(WithRetryInterval(time.Second), WithRetryBackoff(2.0), WithMaxRetryInterval(60*time.Second))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = computeRetryDelay(opts, 5)
	}
}

func BenchmarkItemScore(b *testing.B) {
	now := unix()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = itemScore(now, int32(i))
	}
}

// 防止编译器优化掉 unused
var _ = errors.New
var _ = runtime.NumCPU

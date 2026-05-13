package delayq

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChaos_PushCancelClose 高并发下 Push / Cancel / Close 交错操作
// 仅验证不发生 race / panic / deadlock，不断言计数正确
func TestChaos_PushCancelClose(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}
	const goroutines = 8
	const opsPerGoroutine = 500

	tp := NewMemoryTopicQueue(context.Background(), "chaos",
		WithLogger(NopLogger()),
		WithMaxConcurrency(32),
		WithRetryTimes(0),
	)
	if err := tp.Start(func(item *Item) error {
		// 1% panic 触发 dead letter
		if rand.Intn(100) == 0 {
			panic("chaos panic")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	stop := time.Now().Add(2 * time.Second)
	var pushed, canceled int64

	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerGoroutine && time.Now().Before(stop); i++ {
				switch r.Intn(4) {
				case 0, 1: // Push
					v := []byte{byte(seed), byte(i)}
					if err := tp.Push(&Item{
						DelaySecond: int64(r.Intn(3)),
						Value:       v,
						Priority:    int32(r.Intn(5)),
					}); err == nil {
						atomic.AddInt64(&pushed, 1)
					}
				case 2: // Cancel
					v := []byte{byte(seed), byte(r.Intn(opsPerGoroutine))}
					if c, _ := tp.Cancel(v); c {
						atomic.AddInt64(&canceled, 1)
					}
				case 3: // Get
					_, _, _ = tp.Get([]byte{byte(seed), byte(i)})
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tp.Drain(ctx); err != nil {
		// drain 超时也 OK，关键是不 panic
		t.Logf("drain returned: %v", err)
	}
	_ = tp.Close()

	t.Logf("chaos done: pushed=%d canceled=%d", atomic.LoadInt64(&pushed), atomic.LoadInt64(&canceled))
}

// TestChaos_StartCloseRestart 反复 Start/Close 同一 TopicQueue
func TestChaos_StartCloseRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}
	for i := 0; i < 20; i++ {
		tp := NewMemoryTopicQueue(context.Background(), "chaos2", WithLogger(NopLogger()))
		if err := tp.Start(noopHandler); err != nil {
			t.Fatal(err)
		}
		// 推几个 item
		for j := 0; j < 5; j++ {
			_ = tp.Push(&Item{DelaySecond: 0, Value: []byte{byte(i), byte(j)}})
		}
		if err := tp.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestChaos_ConcurrentDrainPush Drain 期间持续 Push 应被拒绝且不 panic
func TestChaos_ConcurrentDrainPush(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "chaos-drain",
		WithLogger(NopLogger()),
		WithMaxConcurrency(4),
	)
	defer tp.Close()
	if err := tp.Start(noopHandler); err != nil {
		t.Fatal(err)
	}

	// 推一些 1s 后到期的 item，使 drain 进入活跃等待
	for i := 0; i < 20; i++ {
		_ = tp.Push(&Item{DelaySecond: 1, Value: []byte{byte(i)}})
	}

	// 启动 drain
	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		drainDone <- tp.Drain(ctx)
	}()

	// 同时持续 Push（应大部分被 ErrDraining 拒绝）
	var rejected int64
	stop := time.Now().Add(2 * time.Second)
	for time.Now().Before(stop) {
		err := tp.Push(&Item{DelaySecond: 1, Value: []byte("late")})
		if err != nil {
			atomic.AddInt64(&rejected, 1)
		}
	}

	if err := <-drainDone; err != nil {
		t.Logf("drain returned: %v", err)
	}
	t.Logf("concurrent push rejected=%d", atomic.LoadInt64(&rejected))
	if atomic.LoadInt64(&rejected) == 0 {
		t.Fatal("expected at least some pushes to be rejected during drain")
	}
}

// TestChaos_ParallelTopicQueues 多个 TopicQueue 并发 Start/Push/Close
func TestChaos_ParallelTopicQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}
	q := New(WithLogger(NopLogger()), WithMaxConcurrency(16))
	defer q.Close()

	const numTopics = 8
	var wg sync.WaitGroup
	for i := 0; i < numTopics; i++ {
		topic := string([]byte{byte('a' + i)})
		if err := q.Start(topic, noopHandler); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < numTopics; i++ {
		topic := string([]byte{byte('a' + i)})
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = q.Push(&Item{Topic: topic, DelaySecond: int64(j % 2), Value: []byte{byte(j)}})
				if j%10 == 0 {
					_, _, _ = q.Get(topic, []byte{byte(j)})
				}
			}
		}(topic)
	}
	wg.Wait()

	// drain 全部
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Drain(ctx); err != nil {
		t.Logf("drain: %v", err)
	}
}

package delayq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemq_PushOnClosed 验证关闭后 Push 返回错误
func TestMemq_PushOnClosed(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "closed")
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1}); err != ErrTopicQueueHasClosed {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// TestMemq_StartTwice 验证重复 Start 返回错误
func TestMemq_StartTwice(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "dup")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Start(func(item *Item) error { return nil }); err != ErrTopicQueueHasStarted {
		t.Fatalf("want ErrTopicQueueHasStarted got %v", err)
	}
}

// TestMemq_NegativeDelay 验证负 delay 会被当成 0
func TestMemq_NegativeDelay(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	tp := NewMemoryTopicQueue(context.Background(), "neg")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error {
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// DelaySecond=-5 应等价于 0（尽快执行）
	if err := tp.Push(&Item{DelaySecond: -5, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	waitGroupTimeout(t, &wg, 3*time.Second)
}

// TestMemq_LargeDelay_MultiCycle 验证跨时间轮周期的延迟
func TestMemq_LargeDelay_MultiCycle(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "cycle")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// 超过 wheelSize(3600) 的 delay，走 cycle>0 分支
	if err := tp.Push(&Item{DelaySecond: 7200, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 1 {
		t.Fatalf("want length=1 got=%d", l)
	}
}

// TestMemq_PanicRecover 验证 handler panic 会被 recover 并走失败分支
func TestMemq_PanicRecover(t *testing.T) {
	var deadCh = make(chan *Item, 1)
	tp := NewMemoryTopicQueue(context.Background(), "panic",
		WithRetryTimes(0),
		WithOnDeadLetter(func(item *Item) { deadCh <- item }),
	)
	defer tp.Close()
	var called int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&called, 1)
		panic("kaboom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("p")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deadCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("panic should end up as dead letter, called=%d", called)
	}
}

// TestMemq_MaxConcurrency 验证信号量限制实际并发不超过 N
func TestMemq_MaxConcurrency(t *testing.T) {
	const limit = 4
	tp := NewMemoryTopicQueue(context.Background(), "conc", WithMaxConcurrency(limit))
	defer tp.Close()

	var inflight, peak int32
	var wg sync.WaitGroup
	const N = 50
	wg.Add(N)
	if err := tp.Start(func(item *Item) error {
		cur := atomic.AddInt32(&inflight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		if err := tp.Push(&Item{DelaySecond: 1, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitGroupTimeout(t, &wg, 30*time.Second)
	if atomic.LoadInt32(&peak) > int32(limit) {
		t.Fatalf("peak concurrency exceeded limit: peak=%d limit=%d", peak, limit)
	}
	if atomic.LoadInt32(&peak) < 2 {
		t.Fatalf("concurrency did not scale up, peak=%d", peak)
	}
}

// TestMemq_UnlimitedConcurrency 验证 MaxConcurrency<=0 时不限制
func TestMemq_UnlimitedConcurrency(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "unl", WithMaxConcurrency(0))
	defer tp.Close()

	var wg sync.WaitGroup
	const N = 20
	wg.Add(N)
	if err := tp.Start(func(item *Item) error {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		if err := tp.Push(&Item{DelaySecond: 1}); err != nil {
			t.Fatal(err)
		}
	}
	waitGroupTimeout(t, &wg, 10*time.Second)
}

// TestMemq_ContextCancel_ClosesTickers 验证外部 ctx 取消后 ticker 退出，再 Close 返回已关闭
func TestMemq_ContextCancel_ClosesTickers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tp := NewMemoryTopicQueue(ctx, "ctx")
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cancel()
	// 给 ticker goroutine 时间感知 ctx 取消
	waitUntil(t, 2000, func() bool {
		return tp.Close() == ErrTopicQueueHasClosed
	})
}

// TestMemq_MultipleTopics 验证同一组 options 下多个 topic 队列互不干扰
func TestMemq_MultipleTopics(t *testing.T) {
	var wgA, wgB sync.WaitGroup
	wgA.Add(3)
	wgB.Add(2)
	a := NewMemoryTopicQueue(context.Background(), "A")
	b := NewMemoryTopicQueue(context.Background(), "B")
	defer a.Close()
	defer b.Close()
	if err := a.Start(func(item *Item) error { wgA.Done(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(func(item *Item) error { wgB.Done(); return nil }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_ = a.Push(&Item{DelaySecond: 1, Value: []byte{byte(i)}})
	}
	for i := 0; i < 2; i++ {
		_ = b.Push(&Item{DelaySecond: 1, Value: []byte{byte(i)}})
	}
	waitGroupTimeout(t, &wgA, 5*time.Second)
	waitGroupTimeout(t, &wgB, 5*time.Second)
	if a.Topic() != "A" || b.Topic() != "B" {
		t.Fatalf("topic names wrong: %s %s", a.Topic(), b.Topic())
	}
}

// TestMemq_RetrySuccessBeforeDeadletter 验证重试过程中成功会停止重试
func TestMemq_RetrySuccessBeforeDeadletter(t *testing.T) {
	var attempts int32
	var deadCh = make(chan *Item, 1)
	tp := NewMemoryTopicQueue(context.Background(), "retry-ok",
		WithRetryTimes(5),
		WithOnDeadLetter(func(item *Item) { deadCh <- item }),
	)
	defer tp.Close()
	if err := tp.Start(func(item *Item) error {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			return fmt.Errorf("fail %d", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deadCh:
		t.Fatalf("should not produce dead letter, attempts=%d", attempts)
	case <-time.After(6 * time.Second):
		// 超时即符合预期（没有死信）
	}
	if atomic.LoadInt32(&attempts) < 3 {
		t.Fatalf("expect >=3 attempts got=%d", attempts)
	}
}

// waitGroupTimeout 等 WaitGroup 完成或报错超时
func waitGroupTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	ch := make(chan struct{})
	go func() { wg.Wait(); close(ch) }()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timeout after %v", d)
	}
}

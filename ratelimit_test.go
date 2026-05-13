package delayq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ===== tokenBucket 单元测试 =====

func TestTokenBucket_NilDisabled(t *testing.T) {
	if newTokenBucket(0, 10) != nil {
		t.Fatal("rate=0 should yield nil bucket")
	}
	if newTokenBucket(10, 0) != nil {
		t.Fatal("burst=0 should yield nil bucket")
	}
	var b *tokenBucket
	if !b.Allow() {
		t.Fatal("nil bucket Allow should be true")
	}
	if !b.AllowN(100) {
		t.Fatal("nil bucket AllowN should be true")
	}
}

func TestTokenBucket_BurstThenRefill(t *testing.T) {
	original := nowFunc
	defer func() { nowFunc = original }()
	now := time.Unix(0, 0)
	nowFunc = func() time.Time { return now }

	b := newTokenBucket(10, 5) // 10 tokens/s, burst=5
	// 立即可消费 5 个
	for i := 0; i < 5; i++ {
		if !b.Allow() {
			t.Fatalf("burst Allow #%d should succeed", i)
		}
	}
	// 第 6 个应失败
	if b.Allow() {
		t.Fatal("6th Allow should fail")
	}
	// 推进 0.2s 应补 2 个 token
	now = now.Add(200 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("after 0.2s should refill 1st token")
	}
	if !b.Allow() {
		t.Fatal("after 0.2s should refill 2nd token")
	}
	if b.Allow() {
		t.Fatal("3rd Allow after 0.2s should fail")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	original := nowFunc
	defer func() { nowFunc = original }()
	now := time.Unix(0, 0)
	nowFunc = func() time.Time { return now }

	b := newTokenBucket(100, 50)
	if !b.AllowN(50) {
		t.Fatal("AllowN(50) should succeed initially")
	}
	if b.AllowN(1) {
		t.Fatal("AllowN(1) after exhaust should fail")
	}
	if b.AllowN(0) != true {
		t.Fatal("AllowN(0) should always be true")
	}
}

// ===== F4: Push 限流 =====

func TestPush_RateLimited(t *testing.T) {
	original := nowFunc
	defer func() { nowFunc = original }()
	now := time.Unix(0, 0)
	nowFunc = func() time.Time { return now }

	tp := NewMemoryTopicQueue(context.Background(), "rl",
		WithLogger(NopLogger()),
		WithPushRatePerSec(10),
		WithPushBurst(3),
	)
	defer tp.Close()
	if err := tp.Start(noopHandler); err != nil {
		t.Fatal(err)
	}
	// 前 3 次成功
	for i := 0; i < 3; i++ {
		if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte("x")}); err != nil {
			t.Fatalf("push #%d should succeed, got %v", i, err)
		}
	}
	// 第 4 次被限流
	if err := tp.Push(&Item{DelaySecond: 3600, Value: []byte("x")}); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("4th push should be rate limited, got %v", err)
	}
}

func TestPushBatch_RateLimited(t *testing.T) {
	original := nowFunc
	defer func() { nowFunc = original }()
	now := time.Unix(0, 0)
	nowFunc = func() time.Time { return now }

	tp := NewMemoryTopicQueue(context.Background(), "rlb",
		WithLogger(NopLogger()),
		WithPushRatePerSec(10),
		WithPushBurst(5),
	)
	defer tp.Close()
	if err := tp.Start(noopHandler); err != nil {
		t.Fatal(err)
	}
	// 一批 10 个，超过 burst=5，整批拒绝
	items := make([]*Item, 10)
	for i := range items {
		items[i] = &Item{DelaySecond: 3600, Value: []byte("v")}
	}
	if err := tp.PushBatch(items); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("batch should be rate limited, got %v", err)
	}
	// 桶仍是满的，5 个的 batch 可以通过
	smaller := items[:5]
	if err := tp.PushBatch(smaller); err != nil {
		t.Fatalf("5-item batch should pass, got %v", err)
	}
}

func TestPush_RateLimited_Metric(t *testing.T) {
	original := nowFunc
	defer func() { nowFunc = original }()
	now := time.Unix(0, 0)
	nowFunc = func() time.Time { return now }

	var rejected int64
	tp := NewMemoryTopicQueue(context.Background(), "rl-m",
		WithLogger(NopLogger()),
		WithPushRatePerSec(1),
		WithPushBurst(1),
		WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
			if metric == MetricRateLimited {
				atomic.AddInt64(&rejected, value)
			}
		}),
	)
	defer tp.Close()
	if err := tp.Start(noopHandler); err != nil {
		t.Fatal(err)
	}
	_ = tp.Push(&Item{DelaySecond: 3600, Value: []byte("a")}) // ok
	_ = tp.Push(&Item{DelaySecond: 3600, Value: []byte("b")}) // rejected
	_ = tp.Push(&Item{DelaySecond: 3600, Value: []byte("c")}) // rejected
	if v := atomic.LoadInt64(&rejected); v != 2 {
		t.Fatalf("want rejected=2 got=%d", v)
	}
}

// ===== F5: Drain =====

func TestDrain_RejectsNewPush(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "drain-rej", WithLogger(NopLogger()))
	defer tp.Close()
	if err := tp.Start(noopHandler); err != nil {
		t.Fatal(err)
	}
	// 用一个 1s 后到期的 item 让 drain 进入活跃等待状态
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("seed")}); err != nil {
		t.Fatal(err)
	}
	mq := tp.(*memQueue)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- tp.Drain(context.Background())
	}()

	// 等 drain 把 draining 标志置为 1
	waitUntil(t, 2000, func() bool { return mq.draining.Get() == 1 })

	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); !errors.Is(err, ErrDraining) {
		t.Fatalf("push during drain should return ErrDraining, got %v", err)
	}
	// drain 应在 ~1s 内完成
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain should complete in time")
	}
}

func TestDrain_WaitsForExistingItems(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "drain-wait", WithLogger(NopLogger()))
	defer tp.Close()
	var processed int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 推 3 个 1s 后到期的 item
	for i := 0; i < 3; i++ {
		if err := tp.Push(&Item{DelaySecond: 1, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	// Drain 必须等待全部 item 派发
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tp.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if v := atomic.LoadInt32(&processed); v != 3 {
		t.Fatalf("want all 3 processed, got %d", v)
	}
}

func TestDrain_CtxCancel(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "drain-ctx", WithLogger(NopLogger()))
	defer tp.Close()
	if err := tp.Start(func(item *Item) error {
		time.Sleep(2 * time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := tp.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded got %v", err)
	}
}

func TestQueue_CloseGracefully(t *testing.T) {
	q := New(WithLogger(NopLogger()))
	var processed int32
	if err := q.Start("g", func(item *Item) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := q.Push(&Item{Topic: "g", DelaySecond: 1, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.CloseGracefully(ctx); err != nil {
		t.Fatal(err)
	}
	if v := atomic.LoadInt32(&processed); v != 3 {
		t.Fatalf("want all processed, got %d", v)
	}
	// CloseGracefully 后 Push 应失败
	if err := q.Push(&Item{Topic: "g", Value: []byte("late")}); err == nil {
		t.Fatal("push after close should fail")
	}
}

func TestQueue_Drain_MultipleTopics(t *testing.T) {
	q := New(WithLogger(NopLogger()))
	defer q.Close()
	var counts sync.Map
	for _, topic := range []string{"a", "b", "c"} {
		topic := topic
		var c int64
		counts.Store(topic, &c)
		if err := q.Start(topic, func(item *Item) error {
			v, _ := counts.Load(topic)
			atomic.AddInt64(v.(*int64), 1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := q.Push(&Item{Topic: topic, DelaySecond: 1, Value: []byte{byte(i)}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{"a", "b", "c"} {
		v, _ := counts.Load(topic)
		if c := atomic.LoadInt64(v.(*int64)); c != 2 {
			t.Errorf("topic=%s want 2 got %d", topic, c)
		}
	}
}

func TestRedisQueue_Drain(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "rdrain", WithRedisScriptBuilder(newTestBuilder(t)))
	defer tp.Close()
	doneCh := make(chan struct{}, 5)
	if err := tp.Start(func(item *Item) error {
		doneCh <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tp.Push(&Item{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := tp.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	// 至少 3 次派发
	count := 0
loop:
	for {
		select {
		case <-doneCh:
			count++
			if count == 3 {
				break loop
			}
		default:
			break loop
		}
	}
	if count < 3 {
		t.Fatalf("want >=3 drained got %d", count)
	}
	// Drain 不关闭队列，标志 release 后可继续 Push
	if err := tp.Push(&Item{Value: []byte("after-drain")}); err != nil {
		t.Fatalf("push after drain should succeed (queue not closed): %v", err)
	}
}

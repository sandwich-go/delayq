package delayq

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ===== B7: Topic 路由 =====

// TestQueue_Push_TopicRouting_Mismatch Push 时 Item.Topic 指定的 topic 不存在 → ErrTopicQueueHasClosed
func TestQueue_Push_TopicRouting_Mismatch(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("a", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := q.Push(&Item{Topic: "b", Value: []byte("x")})
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// TestQueue_Push_TopicAutoInject 仅注册一个 topic 时，Item.Topic 为空自动注入并能被 handler 看到
func TestQueue_Push_TopicAutoInject(t *testing.T) {
	q := New()
	defer q.Close()

	var observedTopic string
	var wg sync.WaitGroup
	wg.Add(1)
	if err := q.Start("only", func(item *Item) error {
		observedTopic = item.GetTopic()
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	item := &Item{DelaySecond: 1, Value: []byte("x")}
	if err := q.Push(item); err != nil {
		t.Fatal(err)
	}
	if item.GetTopic() != "only" {
		t.Fatalf("Push 应回填 Item.Topic, got %q", item.GetTopic())
	}
	waitWithTimeout(t, &wg, 5*time.Second)
	if observedTopic != "only" {
		t.Fatalf("handler 看到的 topic = %q, want only", observedTopic)
	}
}

// TestQueue_Push_TopicEmpty_MultipleTopics Item.Topic 为空 + 多 topic → 拒绝
func TestQueue_Push_TopicEmpty_MultipleTopics(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("a", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Start("b", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := q.Push(&Item{Value: []byte("x")})
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// TestQueue_Push_TopicEmpty_NoTopic Item.Topic 为空 + 0 个 topic → 拒绝
func TestQueue_Push_TopicEmpty_NoTopic(t *testing.T) {
	q := New()
	defer q.Close()
	err := q.Push(&Item{Value: []byte("x")})
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// TestTopicQueue_Push_NormalizesTopic 直接使用 TopicQueue.Push 时，错误 topic 会被覆盖
func TestTopicQueue_Push_NormalizesTopic(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "real")
	defer tp.Close()
	var observed string
	var wg sync.WaitGroup
	wg.Add(1)
	if err := tp.Start(func(item *Item) error {
		observed = item.GetTopic()
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{Topic: "wrong", DelaySecond: 1}); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, &wg, 5*time.Second)
	if observed != "real" {
		t.Fatalf("topic should be normalized to %q, got %q", "real", observed)
	}
}

// TestTopicQueue_Push_NilItem nil item 返回 ErrNilItem
func TestTopicQueue_Push_NilItem(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "x")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(nil); !errors.Is(err, ErrNilItem) {
		t.Fatalf("want ErrNilItem got %v", err)
	}
}

// ===== B11: Value 大小警告 =====

// TestPush_LargeValue_LogsWarning 推送大 value 应该记 WARN 但不拒绝
func TestPush_LargeValue_LogsWarning(t *testing.T) {
	fl := &fakeLogger{}
	tp := NewMemoryTopicQueue(context.Background(), "big", WithLogger(fl))
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// 5KB value 应该触发 WARN
	big := bytes.Repeat([]byte{'a'}, 5*1024)
	if err := tp.Push(&Item{DelaySecond: 1, Value: big}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fl.String(), "large item value") {
		t.Fatalf("expected large value warning, log:\n%s", fl.String())
	}
}

// TestPush_SmallValue_NoWarning 推送小 value 不应有 WARN
func TestPush_SmallValue_NoWarning(t *testing.T) {
	fl := &fakeLogger{}
	tp := NewMemoryTopicQueue(context.Background(), "small", WithLogger(fl))
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("tiny")}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fl.String(), "large item value") {
		t.Fatalf("unexpected warning, log:\n%s", fl.String())
	}
}

// ===== B9: ticker 退避 =====

func TestBackoffDuration(t *testing.T) {
	cases := []struct {
		base     time.Duration
		failures int
		max      time.Duration
		want     time.Duration
	}{
		{1 * time.Second, 0, 30 * time.Second, 1 * time.Second},
		{1 * time.Second, 1, 30 * time.Second, 1 * time.Second},
		{1 * time.Second, 2, 30 * time.Second, 2 * time.Second},
		{1 * time.Second, 3, 30 * time.Second, 4 * time.Second},
		{1 * time.Second, 5, 30 * time.Second, 16 * time.Second},
		{1 * time.Second, 6, 30 * time.Second, 30 * time.Second}, // cap
		{1 * time.Second, 100, 30 * time.Second, 30 * time.Second},
		{0, 3, 10 * time.Second, 4 * time.Second}, // base 默认 1s
	}
	for _, c := range cases {
		if got := backoffDuration(c.base, c.failures, c.max); got != c.want {
			t.Errorf("backoffDuration(%v,%d,%v) want=%v got=%v", c.base, c.failures, c.max, c.want, got)
		}
	}
}

// TestTicker_BackoffOnError ticker 连续失败时应该真的延迟而非立即重试
func TestTicker_BackoffOnError(t *testing.T) {
	b := &fakeScriptBuilder{}
	fl := &fakeLogger{}
	tp := NewRedisTopicQueue(context.Background(), "tick-bo",
		WithRedisScriptBuilder(b),
		WithLogger(fl),
	)
	rq := tp.(*redisQueue)

	var pollCalls int32
	// move 总是失败
	b.scripts[idxMove].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.AddInt32(&pollCalls, 1)
		return nil, errBoom
	}
	b.scripts[idxMove].evalFn = b.scripts[idxMove].evalShaFn

	if err := rq.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	defer rq.Close()

	// 在 5 秒内本来 1s ticker 会调用 5 次。退避后应该明显少于 5 次（前几次会快速失败累积退避）
	time.Sleep(5 * time.Second)
	calls := atomic.LoadInt32(&pollCalls)
	// poll + reclaim 各失败 → 先粗暴下界保证退避真发生：calls 不能 >> 10（不退避会到 10）
	if calls > 8 {
		t.Fatalf("backoff seems not active, calls=%d in 5s", calls)
	}
	if calls < 1 {
		t.Fatalf("ticker never ran, calls=%d", calls)
	}
}

// TestTicker_RecoveryAfterError 错误恢复后 backoff 应该被重置
func TestTicker_RecoveryAfterError(t *testing.T) {
	b := &fakeScriptBuilder{}
	fl := &fakeLogger{}
	tp := NewRedisTopicQueue(context.Background(), "tick-rec",
		WithRedisScriptBuilder(b),
		WithLogger(fl),
	)
	rq := tp.(*redisQueue)

	var failPhase int32 = 1 // 前 2 次失败，之后成功
	var calls int32
	b.scripts[idxMove].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		c := atomic.AddInt32(&calls, 1)
		if c <= 2 && atomic.LoadInt32(&failPhase) == 1 {
			return nil, errBoom
		}
		return []interface{}{}, nil
	}

	if err := rq.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	defer rq.Close()

	// 等到 ticker 进入恢复期
	waitUntil(t, 10*1000, func() bool {
		return strings.Contains(fl.String(), "ticker recovered")
	})
}

// ===== B10: OnDeadLetter panic 保护 =====

// TestOnDeadLetter_Panic_Recovered OnDeadLetter 回调 panic 不应影响队列继续处理
func TestOnDeadLetter_Panic_Recovered(t *testing.T) {
	fl := &fakeLogger{}
	var deadCalls int32
	tp := NewMemoryTopicQueue(context.Background(), "dl-panic",
		WithLogger(fl),
		WithRetryTimes(0),
		WithOnDeadLetter(func(item *Item) {
			atomic.AddInt32(&deadCalls, 1)
			panic("dead letter callback boom")
		}),
	)
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return errBoom }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	// 等待 dead letter 被调用
	waitUntil(t, 5000, func() bool { return atomic.LoadInt32(&deadCalls) >= 1 })
	// 等待 panic 被记录
	waitUntil(t, 2000, func() bool {
		return strings.Contains(fl.String(), "OnDeadLetter callback panic")
	})

	// 再 Push 一次，验证队列依然能工作
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x2")}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5000, func() bool { return atomic.LoadInt32(&deadCalls) >= 2 })
}

// TestOnDeadLetter_NotSet 未设置 OnDeadLetter 时仅打 WARN 日志，不 panic
func TestOnDeadLetter_NotSet(t *testing.T) {
	fl := &fakeLogger{}
	tp := NewMemoryTopicQueue(context.Background(), "dl-nil",
		WithLogger(fl),
		WithRetryTimes(0),
	)
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return errBoom }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5000, func() bool {
		return strings.Contains(fl.String(), "dead letter")
	})
}

// ===== B8: execute 信号量边界 =====

// TestExecute_NoLeakOnClose Close 期间 execute 不应残留 goroutine
func TestExecute_NoLeakOnClose(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "exec-leak", WithMaxConcurrency(2))
	var inflight int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&inflight, 1)
		time.Sleep(300 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_ = tp.Push(&Item{DelaySecond: 1})
	}
	// 等到 ticker 派发
	time.Sleep(1100 * time.Millisecond)
	// Close 必须等所有 inflight 回零
	if err := tp.Close(); err != nil {
		t.Fatal(err)
	}
	if v := atomic.LoadInt32(&inflight); v != 0 {
		t.Fatalf("after close inflight should be 0, got %d", v)
	}
}

// TestExecute_HandlerPanic_ContinueProcessing handler panic 后队列应继续工作
func TestExecute_HandlerPanic_ContinueProcessing(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "panic-cont", WithRetryTimes(0))
	defer tp.Close()

	var ok int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := tp.Start(func(item *Item) error {
		switch string(item.GetValue()) {
		case "panic":
			panic("oops")
		case "ok":
			atomic.AddInt32(&ok, 1)
			wg.Done()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("panic")}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, &wg, 5*time.Second)
	if atomic.LoadInt32(&ok) != 1 {
		t.Fatalf("want ok=1 got=%d", ok)
	}
}

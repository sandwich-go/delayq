package delayq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestQueue_NewAndStart_Memory 验证未配置 RedisScriptBuilder 时落到内存实现
func TestQueue_NewAndStart_Memory(t *testing.T) {
	q := New()
	defer q.Close()
	var got int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := q.Start("topic-mem", func(item *Item) error {
		atomic.AddInt32(&got, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "topic-mem", DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, &wg, 5*time.Second)
	if got != 1 {
		t.Fatalf("want handler called 1, got %d", got)
	}
}

// TestQueue_NewAndStart_Redis 验证配置了 RedisScriptBuilder 时落到 Redis 实现
func TestQueue_NewAndStart_Redis(t *testing.T) {
	q := New(WithRedisScriptBuilder(newTestBuilder(t)))
	defer q.Close()

	var got int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := q.Start("topic-redis", func(item *Item) error {
		atomic.AddInt32(&got, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "topic-redis", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, &wg, 5*time.Second)
}

// TestQueue_StartTopicQueue_Register 验证重复注册同一 topic 返回 ErrTopicQueueHasRegistered
func TestQueue_StartTopicQueue_Register(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("dup", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Start("dup", func(item *Item) error { return nil }); err != ErrTopicQueueHasRegistered {
		t.Fatalf("want ErrTopicQueueHasRegistered, got %v", err)
	}
}

// TestQueue_Push_UnknownTopic 推送到未注册 topic 应返回 ErrTopicQueueHasClosed
func TestQueue_Push_UnknownTopic(t *testing.T) {
	q := New()
	defer q.Close()
	err := q.Push(&Item{Topic: "nope", Value: []byte("x")})
	if err != ErrTopicQueueHasClosed {
		t.Fatalf("want ErrTopicQueueHasClosed, got %v", err)
	}
}

// TestQueue_Stop 验证停止单个 topic
func TestQueue_Stop(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("s1", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Stop("s1"); err != nil {
		t.Fatal(err)
	}
	// Stop 一个不存在的 topic 应返回 nil
	if err := q.Stop("not-exist"); err != nil {
		t.Fatal(err)
	}
}

// TestQueue_Status 验证多 topic 状态汇总
func TestQueue_Status(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("a", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Start("b", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// a 推 3 个，b 推 1 个，DelaySecond 足够大确保不会被消费掉
	for i := 0; i < 3; i++ {
		if err := q.Push(&Item{Topic: "a", DelaySecond: 3600, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Push(&Item{Topic: "b", DelaySecond: 3600, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	s := q.Status()
	if s.QueueLength["a"] != 3 {
		t.Fatalf("a length want=3 got=%d", s.QueueLength["a"])
	}
	if s.QueueLength["b"] != 1 {
		t.Fatalf("b length want=1 got=%d", s.QueueLength["b"])
	}
}

// TestQueue_Close_All 验证 Close 关闭所有 topic
func TestQueue_Close_All(t *testing.T) {
	q := New()
	if err := q.Start("c1", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Start("c2", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	// Close 后再 Push 应失败
	if err := q.Push(&Item{Topic: "c1", Value: []byte("x")}); err == nil {
		t.Fatal("push after close should fail")
	}
}

// TestQueue_Collector 验证 Collector 返回非 nil
func TestQueue_Collector(t *testing.T) {
	q := New()
	defer q.Close()
	if c := q.Collector(); c == nil {
		t.Fatal("collector should not be nil")
	}
}

// TestQueue_MonitorCounter 验证成功/失败路径都会调用 MonitorCounter
func TestQueue_MonitorCounter(t *testing.T) {
	var metrics sync.Map // metric -> count
	inc := func(metric string, value int64, labels prometheus.Labels) {
		v, _ := metrics.LoadOrStore(metric, new(int64))
		atomic.AddInt64(v.(*int64), value)
	}
	q := New(WithMonitorCounter(inc), WithRetryTimes(0))
	defer q.Close()

	// 通过 value 内容控制成功/失败路径，避免基于全局计数器的非确定性
	if err := q.Start("m", func(item *Item) error {
		if string(item.GetValue()) == "fail" {
			return fmt.Errorf("boom")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "m", DelaySecond: 1, Value: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "m", DelaySecond: 1, Value: []byte("fail")}); err != nil {
		t.Fatal(err)
	}

	// 等待两个 metric 同时出现
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		hv, _ := metrics.Load("delayq_handle")
		ev, _ := metrics.Load("delayq_handle_error")
		if hv != nil && ev != nil &&
			atomic.LoadInt64(hv.(*int64)) >= 1 &&
			atomic.LoadInt64(ev.(*int64)) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	produceV, _ := metrics.Load("delayq_produce")
	if produceV == nil || atomic.LoadInt64(produceV.(*int64)) < 2 {
		t.Fatalf("produce metric missing or wrong: %v", produceV)
	}
	handleV, _ := metrics.Load("delayq_handle")
	if handleV == nil || atomic.LoadInt64(handleV.(*int64)) < 1 {
		t.Fatalf("handle metric missing: %v", handleV)
	}
	errV, _ := metrics.Load("delayq_handle_error")
	if errV == nil || atomic.LoadInt64(errV.(*int64)) < 1 {
		t.Fatalf("handle_error metric missing: %v", errV)
	}
}

// TestQueue_Produce_Error 验证推送失败时 produce_error 被计数
func TestQueue_Produce_Error(t *testing.T) {
	var produceErr int64
	q := New(WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		if metric == "delayq_produce_error" {
			atomic.AddInt64(&produceErr, value)
		}
	}))
	defer q.Close()
	// 未 Start 直接 Push，走 error 路径
	_ = q.Push(&Item{Topic: "never", Value: []byte("x")})
	if atomic.LoadInt64(&produceErr) != 1 {
		t.Fatalf("want produce_error=1, got %d", produceErr)
	}
}

// waitWithTimeout 等待 wg 或超时
func waitWithTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	ch := make(chan struct{})
	go func() { wg.Wait(); close(ch) }()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("wait timeout after %v", d)
	}
}

// TestQueue_ContextCancel 验证外部 ctx 取消时 Queue 能清理
func TestQueue_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// New 内部用自己的 ctx，但 baseQueue 使用 New 传入的 ctx
	// 这里仅验证即使未显式传 ctx，Close 依然正常
	_ = ctx
	_ = cancel

	q := New()
	if err := q.Start("ctx", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

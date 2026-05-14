//go:build integration

// Package delayq integration tests against a real Redis instance.
//
// Run locally:
//
//	REDIS_ADDR=127.0.0.1:6379 go test -tags=integration -run TestIntegration ./...
//
// Run via Docker:
//
//	docker run -d --rm -p 6379:6379 --name delayq-redis redis:7-alpine
//	REDIS_ADDR=127.0.0.1:6379 go test -tags=integration -run TestIntegration ./...
//	docker stop delayq-redis
//
// CI: github actions provides a redis service container, see .github/workflows/ci.yml
package delayq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandwich-go/redisson"
)

func realRedisBuilder(t *testing.T) RedisScriptBuilder {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping integration test")
	}
	c := redisson.MustNewClient(redisson.NewConf(
		redisson.WithAddrs(addr),
		redisson.WithDevelopment(false),
	))
	return redissonScriptBuilder{c: c}
}

// uniqueTopic 给每个测试一个独立 topic，避免相互污染
func uniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("it-%s-%d", t.Name(), time.Now().UnixNano())
}

// TestIntegration_Redis_BasicPushHandle 推 N 条消息，全部被 handler 消费
func TestIntegration_Redis_BasicPushHandle(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	var processed int64
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		if err := tp.Push(&Item{Value: []byte(fmt.Sprintf("msg-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout, processed=%d/%d", atomic.LoadInt64(&processed), N)
	}
}

// TestIntegration_Redis_DelayPrecision 验证延迟精度（粒度为 1s）
func TestIntegration_Redis_DelayPrecision(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	receivedCh := make(chan time.Time, 1)
	if err := tp.Start(func(item *Item) error {
		receivedCh <- time.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	pushed := time.Now()
	if err := tp.Push(&Item{DelaySecond: 2, Value: []byte("delayed")}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-receivedCh:
		elapsed := got.Sub(pushed)
		if elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
			t.Fatalf("delay out of range: %v", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

// TestIntegration_Redis_RetryAndDeadLetter 验证重试与死信
func TestIntegration_Redis_RetryAndDeadLetter(t *testing.T) {
	topic := uniqueTopic(t)
	deadCh := make(chan *Item, 1)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
		WithRetryTimes(2),
		WithRetryInterval(500*time.Millisecond),
		WithOnDeadLetter(func(item *Item) { deadCh <- item }),
	)
	defer tp.Close()

	var attempts int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("always fail")
	}); err != nil {
		t.Fatal(err)
	}

	if err := tp.Push(&Item{Value: []byte("doomed")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deadCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("dead letter timeout, attempts=%d", atomic.LoadInt32(&attempts))
	}
	if a := atomic.LoadInt32(&attempts); a < 2 {
		t.Fatalf("want >=2 attempts, got %d", a)
	}
}

// TestIntegration_Redis_VisibilityReclaim 验证 doing 集 reclaim
func TestIntegration_Redis_VisibilityReclaim(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(2*time.Second),
		WithRetryTimes(0), // 失败直接死信
	)
	defer tp.Close()
	rq := tp.(*redisQueue)

	// 直接放一个过期 item 到 doing 集，模拟"曾被 poll 但未 ack"
	expiredScore := unix() - 10
	if _, err := rq.runScript(context.Background(), rq.addScript,
		[]string{rq.doingSetKey}, []byte("ghost"), expiredScore); err != nil {
		t.Fatal(err)
	}

	receivedCh := make(chan struct{}, 1)
	if err := tp.Start(func(item *Item) error {
		if string(item.GetValue()) == "ghost" {
			receivedCh <- struct{}{}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-receivedCh:
	case <-time.After(8 * time.Second):
		t.Fatal("reclaim timeout")
	}
}

// TestIntegration_Redis_PushBatch 大批量 PushBatch + 全部消费
func TestIntegration_Redis_PushBatch(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	const N = 200
	var wg sync.WaitGroup
	wg.Add(N)
	if err := tp.Start(func(item *Item) error {
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	items := make([]*Item, N)
	for i := range items {
		items[i] = &Item{Value: []byte(fmt.Sprintf("b-%d", i))}
	}
	if err := tp.PushBatch(items); err != nil {
		t.Fatal(err)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(20 * time.Second):
		t.Fatal("batch consume timeout")
	}
}

// TestIntegration_Redis_Cancel 取消未到期的 item
func TestIntegration_Redis_Cancel(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	if err := tp.Push(&Item{DelaySecond: 60, Value: []byte("c1")}); err != nil {
		t.Fatal(err)
	}
	canceled, err := tp.Cancel([]byte("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if !canceled {
		t.Fatal("cancel should succeed")
	}
	if l := tp.Length(); l != 0 {
		t.Fatalf("length should be 0 after cancel, got %d", l)
	}
}

// TestIntegration_Redis_GracefulDrain Drain 等待 doing 集消化
func TestIntegration_Redis_GracefulDrain(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	var processed int64
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt64(&processed, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := tp.Push(&Item{Value: []byte(fmt.Sprintf("d-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tp.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if atomic.LoadInt64(&processed) != 10 {
		t.Fatalf("want 10 processed, got %d", processed)
	}
}

// TestIntegration_Redis_Concurrent 多 goroutine 并发 push + 消费
func TestIntegration_Redis_Concurrent(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
		WithMaxConcurrency(16),
	)
	defer tp.Close()

	const Goroutines = 8
	const PerGoroutine = 25
	const Total = Goroutines * PerGoroutine

	var wg sync.WaitGroup
	wg.Add(Total)
	var processed int64
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var pushWG sync.WaitGroup
	for g := 0; g < Goroutines; g++ {
		pushWG.Add(1)
		go func(g int) {
			defer pushWG.Done()
			for i := 0; i < PerGoroutine; i++ {
				_ = tp.Push(&Item{Value: []byte(fmt.Sprintf("g%d-i%d", g, i))})
			}
		}(g)
	}
	pushWG.Wait()

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(30 * time.Second):
		t.Fatalf("timeout, processed=%d/%d", atomic.LoadInt64(&processed), Total)
	}
}

// TestIntegration_Redis_HeartbeatExtendsVisibility 验证长任务 handler 不会被 reclaim 误判
//
// 配置 VisibilityTimeout=2s, heartbeatInterval=500ms, handler 耗时 5s（远超 2s）。
// 期望：handler 完整执行 1 次（不重复派发）。
func TestIntegration_Redis_HeartbeatExtendsVisibility(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(2*time.Second),
		WithHeartbeatInterval(500*time.Millisecond),
		WithRetryTimes(0),
	)
	defer tp.Close()

	var attempts int32
	doneCh := make(chan struct{}, 1)
	if err := tp.Start(func(item *Item) error {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			// 长任务：5s（远超 visibility=2s）
			time.Sleep(5 * time.Second)
			doneCh <- struct{}{}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := tp.Push(&Item{Value: []byte("long-task")}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("handler timeout, attempts=%d", atomic.LoadInt32(&attempts))
	}

	// 给 reclaim 一些时间观察是否会再次派发
	time.Sleep(3 * time.Second)
	if a := atomic.LoadInt32(&attempts); a != 1 {
		t.Fatalf("heartbeat should prevent re-dispatch, but handler was called %d times", a)
	}
}

// TestIntegration_Redis_HeartbeatDisabled 显式禁用心跳时长任务会被重复派发
//
// 这是反例验证：禁用心跳后 visibility=2s + handler 5s 必定触发 reclaim。
func TestIntegration_Redis_HeartbeatDisabled(t *testing.T) {
	topic := uniqueTopic(t)
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(realRedisBuilder(t)),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(2*time.Second),
		WithHeartbeatInterval(-1), // 显式禁用
		WithRetryTimes(0),
	)
	defer tp.Close()

	var attempts int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&attempts, 1)
		time.Sleep(5 * time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := tp.Push(&Item{Value: []byte("long-no-heartbeat")}); err != nil {
		t.Fatal(err)
	}

	// 等到 reclaim（>2s+一个 ticker 周期）+ 第二次执行启动
	time.Sleep(4 * time.Second)
	if a := atomic.LoadInt32(&attempts); a < 2 {
		t.Fatalf("without heartbeat, expected re-dispatch, got attempts=%d", a)
	}
}

// TestIntegration_Redis_NOSCRIPT_Reload 验证 SCRIPT FLUSH 后 EvalSha → NOSCRIPT → Eval 路径
func TestIntegration_Redis_NOSCRIPT_Reload(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	topic := uniqueTopic(t)
	c := redisson.MustNewClient(redisson.NewConf(
		redisson.WithAddrs(addr),
		redisson.WithDevelopment(false),
	))
	tp := NewRedisTopicQueue(context.Background(), topic,
		WithRedisScriptBuilder(redissonScriptBuilder{c: c}),
		WithLogger(NopLogger()),
	)
	defer tp.Close()

	if err := tp.Push(&Item{Value: []byte("before-flush")}); err != nil {
		t.Fatal(err)
	}
	// 模拟 Redis 重启或脚本被清除
	if err := c.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	// 此 push 触发 NOSCRIPT 然后 Eval 重载，应该成功
	if err := tp.Push(&Item{Value: []byte("after-flush")}); err != nil {
		t.Fatalf("push after script flush should succeed via Eval fallback: %v", err)
	}
	if l := tp.Length(); l < 2 {
		t.Fatalf("length should be >=2, got %d", l)
	}
}

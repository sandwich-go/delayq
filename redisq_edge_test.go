package delayq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeScriptBuilder/fakeScript 用于精准控制 Redis 调用返回，验证 NOSCRIPT fallback / error 路径
type fakeScriptBuilder struct {
	scripts []*fakeScript
}

func (b *fakeScriptBuilder) Build(src string) RedisScript {
	s := &fakeScript{src: src}
	b.scripts = append(b.scripts, s)
	return s
}

type fakeScript struct {
	src string
	mu  sync.Mutex

	evalShaFn func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)
	evalFn    func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error)

	evalShaCalls int
	evalCalls    int
}

func (s *fakeScript) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	s.mu.Lock()
	s.evalShaCalls++
	fn := s.evalShaFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, keys, args...)
	}
	return nil, errors.New("NOSCRIPT  not loaded")
}

func (s *fakeScript) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	s.mu.Lock()
	s.evalCalls++
	fn := s.evalFn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, keys, args...)
	}
	return nil, errors.New("no eval impl")
}

// TestRedisQueue_NOSCRIPT_Fallback 验证 EvalSha 返回 NOSCRIPT 时回退到 Eval
func TestRedisQueue_NOSCRIPT_Fallback(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "nos", WithRedisScriptBuilder(b))
	rq := tp.(*redisQueue)

	// 设置 addScript：EvalSha 返回 NOSCRIPT，Eval 返回成功
	b.scripts[1].evalFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{true}, nil
	}
	if err := rq.Push(&Item{Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if b.scripts[1].evalShaCalls != 1 || b.scripts[1].evalCalls != 1 {
		t.Fatalf("want evalsha=1 eval=1 got evalsha=%d eval=%d", b.scripts[1].evalShaCalls, b.scripts[1].evalCalls)
	}
}

// TestRedisQueue_Length_Error 验证 Length 在脚本错误时返回 0 并记日志
func TestRedisQueue_Length_Error(t *testing.T) {
	b := &fakeScriptBuilder{}
	fl := &fakeLogger{}
	tp := NewRedisTopicQueue(context.Background(), "len-err", WithRedisScriptBuilder(b), WithLogger(fl))
	rq := tp.(*redisQueue)

	// lengthScript 两个方法都失败（非 NOSCRIPT）
	b.scripts[2].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return nil, fmt.Errorf("io error")
	}
	b.scripts[2].evalFn = b.scripts[2].evalShaFn

	if l := rq.Length(); l != 0 {
		t.Fatalf("want 0 got %d", l)
	}
	if !strings.Contains(fl.String(), "length error") {
		t.Fatalf("log should mention length error, got: %s", fl.String())
	}
}

// TestRedisQueue_Length_EmptyResponse 验证 Length 返回空切片时返回 0
func TestRedisQueue_Length_EmptyResponse(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "len-empty", WithRedisScriptBuilder(b))
	rq := tp.(*redisQueue)
	b.scripts[2].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{}, nil
	}
	if l := rq.Length(); l != 0 {
		t.Fatalf("want 0 got %d", l)
	}
}

// TestRedisQueue_Poll_Error 验证 poll 出错时返回错误但不 crash
func TestRedisQueue_Poll_Error(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "poll-err", WithRedisScriptBuilder(b))
	rq := tp.(*redisQueue)
	// moveScript 两路径都返回错误
	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return nil, fmt.Errorf("network")
	}
	b.scripts[0].evalFn = b.scripts[0].evalShaFn

	if err := rq.poll(); err == nil {
		t.Fatal("want error")
	}
	if err := rq.reclaim(); err == nil {
		t.Fatal("want error")
	}
}

// TestRedisQueue_Poll_MalformedResponse 验证 poll 遇到非字符串条目时跳过
func TestRedisQueue_Poll_MalformedResponse(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "poll-bad", WithRedisScriptBuilder(b))
	rq := tp.(*redisQueue)

	var pollInvoked int32
	// move 只在第一次调用返回带坏数据的响应；后续返回空，防止 ticker 多次放大
	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		if atomic.AddInt32(&pollInvoked, 1) == 1 {
			return []interface{}{123, "0", "hello", "0"}, nil
		}
		return []interface{}{}, nil
	}
	// ack 等其他脚本 no-op
	for i := 1; i < len(b.scripts); i++ {
		b.scripts[i].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
			return []interface{}{true}, nil
		}
	}

	var mu sync.Mutex
	var got []string
	gotCh := make(chan struct{}, 1)
	if err := rq.Start(func(item *Item) error {
		mu.Lock()
		got = append(got, string(item.GetValue()))
		mu.Unlock()
		select {
		case gotCh <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer rq.Close()

	// 手动触发 poll
	if err := rq.poll(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gotCh:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) < 1 || got[0] != "hello" {
		t.Fatalf("want [hello] got %v", got)
	}
}

// TestRedisQueue_Poll_DeadLetterBranch 覆盖 poll 中 score<0 达到 RetryTimes 的死信分支
func TestRedisQueue_Poll_DeadLetterBranch(t *testing.T) {
	b := &fakeScriptBuilder{}
	var deadCh = make(chan *Item, 1)
	tp := NewRedisTopicQueue(context.Background(), "poll-dl",
		WithRedisScriptBuilder(b),
		WithRetryTimes(2),
		WithOnDeadLetter(func(item *Item) { deadCh <- item }),
	)
	rq := tp.(*redisQueue)

	// moveScript 返回已失败 score=-5 的 item
	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{"bad", "-5"}, nil
	}
	// ackSuccessScript no-op
	for i := 1; i < len(b.scripts); i++ {
		b.scripts[i].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
			return []interface{}{true}, nil
		}
	}

	if err := rq.poll(); err != nil {
		t.Fatal(err)
	}
	select {
	case it := <-deadCh:
		if string(it.GetValue()) != "bad" {
			t.Fatalf("dead letter value mismatch: %s", it.GetValue())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("dead letter not triggered")
	}
}

// TestRedisQueue_Poll_FailedButUnderRetry 覆盖 score<0 但未达到 RetryTimes 的重新执行分支
func TestRedisQueue_Poll_FailedButUnderRetry(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "poll-retry",
		WithRedisScriptBuilder(b),
		WithRetryTimes(10),
	)
	rq := tp.(*redisQueue)

	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{"retry-me", "-2"}, nil
	}
	for i := 1; i < len(b.scripts); i++ {
		b.scripts[i].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
			return []interface{}{true}, nil
		}
	}

	var handled int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := rq.Start(func(item *Item) error {
		if atomic.AddInt32(&handled, 1) == 1 {
			wg.Done()
		}
		if item.GetDelaySecond() != -2 {
			t.Errorf("expect DelaySecond=-2 got=%d", item.GetDelaySecond())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer rq.Close()

	if err := rq.poll(); err != nil {
		t.Fatal(err)
	}
	waitGroupTimeout(t, &wg, 3*time.Second)
}

// TestRedisQueue_Reclaim_CountsItems 验证 reclaim 成功时根据返回元素数做监控计数
func TestRedisQueue_Reclaim_CountsItems(t *testing.T) {
	b := &fakeScriptBuilder{}
	var reclaimCount int64
	tp := NewRedisTopicQueue(context.Background(), "recl-cnt",
		WithRedisScriptBuilder(b),
		WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
			if metric == "delayq_reclaim" {
				atomic.AddInt64(&reclaimCount, value)
			}
		}),
	)
	rq := tp.(*redisQueue)
	// moveScript 返回 2 个 item（4 个元素）
	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{"a", "0", "b", "0"}, nil
	}
	if err := rq.reclaim(); err != nil {
		t.Fatal(err)
	}
	if v := atomic.LoadInt64(&reclaimCount); v != 2 {
		t.Fatalf("want 2 got %d", v)
	}
}

// TestRedisQueue_OpCtx_NilCtx 覆盖 opCtx 在 q.ctx 为 nil 时返回 background
func TestRedisQueue_OpCtx_NilCtx(t *testing.T) {
	rq := &redisQueue{baseQueue: &baseQueue{ctx: nil}}
	if c := rq.opCtx(); c == nil {
		t.Fatal("opCtx should return non-nil when ctx is nil")
	}
}

// TestRedisQueue_StartWithTwoTickers 验证 Start 起了 poll+reclaim 两个 ticker
func TestRedisQueue_StartWithTwoTickers(t *testing.T) {
	b := &fakeScriptBuilder{}
	for range []int{0, 1, 2, 3, 4} { // 预先 build 所有脚本位占位
	}
	tp := NewRedisTopicQueue(context.Background(), "two-tickers", WithRedisScriptBuilder(b))
	rq := tp.(*redisQueue)

	var moveCalls int32
	b.scripts[0].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.AddInt32(&moveCalls, 1)
		return []interface{}{}, nil
	}
	if err := rq.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	defer rq.Close()
	// 2.2 秒内至少应该有 poll + reclaim 各 1 次以上调用（共 >=4）
	time.Sleep(2200 * time.Millisecond)
	if v := atomic.LoadInt32(&moveCalls); v < 2 {
		t.Fatalf("want at least 2 move calls, got %d", v)
	}
}

package delayq

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRedisQueue_VisibilityTimeout_Used 验证 poll 的 doing 集 score = now + VisibilityTimeout
func TestRedisQueue_VisibilityTimeout_Used(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "vt",
		WithRedisScriptBuilder(b),
		WithVisibilityTimeout(123*time.Second),
	)
	rq := tp.(*redisQueue)

	var captured struct {
		mu    sync.Mutex
		args  []interface{}
		count int
	}
	b.scripts[idxMove].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		captured.mu.Lock()
		captured.args = args
		captured.count++
		captured.mu.Unlock()
		return []interface{}{}, nil
	}

	if err := rq.poll(); err != nil {
		t.Fatal(err)
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if captured.count != 1 || len(captured.args) != 2 {
		t.Fatalf("unexpected captured args: %+v", captured.args)
	}
	now := captured.args[0].(int64)
	target := captured.args[1].(int64)
	if target-now != 123 {
		t.Fatalf("want diff 123, got now=%d target=%d", now, target)
	}
}

// TestRedisQueue_OnFailed_UsesRetryDelay 验证 ackFailed 调用时 score = now + retryDelay
func TestRedisQueue_OnFailed_UsesRetryDelay(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ack-fail",
		WithRedisScriptBuilder(b),
		WithRetryInterval(10*time.Second),
	)
	rq := tp.(*redisQueue)

	var capturedScore int64
	var captured int32
	b.scripts[idxAckFailed].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.StoreInt64(&capturedScore, int64(parseFloat64(args[1])))
		atomic.AddInt32(&captured, 1)
		return []interface{}{int64(1)}, nil
	}

	now := unix()
	if err := rq.onFailed(&Item{Value: []byte("v"), DelaySecond: 0}); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&captured) != 1 {
		t.Fatal("ackFailed not called")
	}
	score := atomic.LoadInt64(&capturedScore)
	diff := score - now
	if diff < 9 || diff > 12 {
		t.Fatalf("score offset out of range: %d (now=%d score=%d)", diff, now, score)
	}
}

// TestRedisQueue_OnFailed_BackoffByFailedCount 验证已失败过的 item 会用更长的退避
func TestRedisQueue_OnFailed_BackoffByFailedCount(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ack-backoff",
		WithRedisScriptBuilder(b),
		WithRetryInterval(1*time.Second),
		WithRetryBackoff(2.0),
		WithMaxRetryInterval(60*time.Second),
	)
	rq := tp.(*redisQueue)

	type capture struct {
		score int64
	}
	captures := make(chan capture, 4)
	b.scripts[idxAckFailed].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		captures <- capture{score: int64(parseFloat64(args[1]))}
		return []interface{}{int64(1)}, nil
	}

	// item.DelaySecond=-3 表示已经失败过 3 次，下一次应是第 4 次失败 -> 1*2^3=8s
	now := unix()
	if err := rq.onFailed(&Item{Value: []byte("v"), DelaySecond: -3}); err != nil {
		t.Fatal(err)
	}
	c := <-captures
	diff := c.score - now
	if diff < 7 || diff > 10 {
		t.Fatalf("backoff out of range: diff=%d", diff)
	}
}

// TestRedisQueue_FailedCountQueryError_Tolerated 失败计数查询出错时仍按 0 派发
func TestRedisQueue_FailedCountQueryError_Tolerated(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "fc-err",
		WithRedisScriptBuilder(b),
		WithRetryTimes(5),
	)
	rq := tp.(*redisQueue)

	stubAllScriptsOK(b)
	// move 返回到期 item；failedCount 故意失败
	b.scripts[idxMove].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return []interface{}{"v1", "0"}, nil
	}
	b.scripts[idxFailedCount].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		return nil, errBoom
	}

	var handled int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := rq.Start(func(item *Item) error {
		if atomic.AddInt32(&handled, 1) == 1 {
			wg.Done()
		}
		// 既然查询失败按 0 处理，DelaySecond 应为 0
		if item.GetDelaySecond() != 0 {
			t.Errorf("want DelaySecond=0 got=%d", item.GetDelaySecond())
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

// TestRedisQueue_PoolReclaimUsesNow 验证 reclaim 现在使用 now 作为 max_priority（不再依赖 visibility）
func TestRedisQueue_PoolReclaimUsesNow(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "rec-now",
		WithRedisScriptBuilder(b),
		WithVisibilityTimeout(60*time.Second),
	)
	rq := tp.(*redisQueue)

	var maxScore int64
	b.scripts[idxMove].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.StoreInt64(&maxScore, args[0].(int64))
		return []interface{}{}, nil
	}
	if err := rq.reclaim(); err != nil {
		t.Fatal(err)
	}
	now := unix()
	mp := atomic.LoadInt64(&maxScore)
	if mp < now-2 || mp > now+2 {
		t.Fatalf("reclaim should use now as max priority, got %d (now=%d)", mp, now)
	}
}

// TestRedisQueue_PushWithDelay_ScoreFuture 验证 Push 的 score = now + DelaySecond
func TestRedisQueue_PushWithDelay_ScoreFuture(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "push-future",
		WithRedisScriptBuilder(b),
	)
	rq := tp.(*redisQueue)

	var capturedScore int64
	b.scripts[idxAdd].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.StoreInt64(&capturedScore, int64(parseFloat64(args[1])))
		return []interface{}{true}, nil
	}

	now := unix()
	if err := rq.Push(&Item{DelaySecond: 30, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	score := atomic.LoadInt64(&capturedScore)
	diff := score - now
	if diff < 29 || diff > 31 {
		t.Fatalf("score not future, diff=%d", diff)
	}
}

// TestRedisQueue_PushNegativeDelay_TreatedAsZero 负 DelaySecond 等价于立即可执行
func TestRedisQueue_PushNegativeDelay_TreatedAsZero(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "push-neg",
		WithRedisScriptBuilder(b),
	)
	rq := tp.(*redisQueue)

	var capturedScore int64
	b.scripts[idxAdd].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.StoreInt64(&capturedScore, int64(parseFloat64(args[1])))
		return []interface{}{true}, nil
	}

	now := unix()
	if err := rq.Push(&Item{DelaySecond: -100, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	score := atomic.LoadInt64(&capturedScore)
	if score < now-2 || score > now+2 {
		t.Fatalf("negative delay should yield ~now, got %d (now=%d)", score, now)
	}
}

// TestParseInt64 验证 parseInt64 兼容多种类型
func TestParseInt64(t *testing.T) {
	cases := []struct {
		in   interface{}
		want int64
	}{
		{int64(42), 42},
		{int(42), 42},
		{"42", 42},
		{"-7", -7},
		{"abc", 0}, // 不可解析返回 0
		{nil, 0},
		{true, 0},
	}
	for _, c := range cases {
		if got := parseInt64(c.in); got != c.want {
			t.Errorf("parseInt64(%v) want=%d got=%d", c.in, c.want, got)
		}
	}
	// 防止编译器警告 strconv 未使用
	_ = strconv.Itoa(0)
}

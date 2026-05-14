package delayq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestHeartbeatInterval_Default 默认 = VisibilityTimeout/3, 至少 1s
func TestHeartbeatInterval_Default(t *testing.T) {
	cases := []struct {
		vt   time.Duration
		want time.Duration
	}{
		{30 * time.Second, 10 * time.Second},
		{60 * time.Second, 20 * time.Second},
		{600 * time.Second, 200 * time.Second},
		{2 * time.Second, 1 * time.Second}, // 至少 1s
		{0, 0},
		{-1, 0},
	}
	for _, c := range cases {
		b := &fakeScriptBuilder{}
		tp := NewRedisTopicQueue(context.Background(), "ht-default",
			WithRedisScriptBuilder(b),
			WithVisibilityTimeout(c.vt),
		)
		rq := tp.(*redisQueue)
		if got := rq.heartbeatInterval(); got != c.want {
			t.Errorf("vt=%v want=%v got=%v", c.vt, c.want, got)
		}
	}
}

// TestHeartbeatInterval_Disabled 显式 -1 禁用心跳
func TestHeartbeatInterval_Disabled(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ht-disabled",
		WithRedisScriptBuilder(b),
		WithVisibilityTimeout(60*time.Second),
		WithHeartbeatInterval(-1),
	)
	rq := tp.(*redisQueue)
	if got := rq.heartbeatInterval(); got != 0 {
		t.Errorf("want disabled (0), got %v", got)
	}
	if rq.onItemStart != nil {
		t.Error("onItemStart should not be set when heartbeat disabled")
	}
}

// TestHeartbeatInterval_Custom 自定义间隔
func TestHeartbeatInterval_Custom(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ht-custom",
		WithRedisScriptBuilder(b),
		WithHeartbeatInterval(7*time.Second),
	)
	rq := tp.(*redisQueue)
	if got := rq.heartbeatInterval(); got != 7*time.Second {
		t.Errorf("want 7s got %v", got)
	}
}

// TestHeartbeat_RefreshesScore 验证 handler 长时间执行时 heartbeat 会调用 heartbeatScript
func TestHeartbeat_RefreshesScore(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ht-refresh",
		WithRedisScriptBuilder(b),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(3*time.Second),
		WithHeartbeatInterval(100*time.Millisecond),
	)
	defer tp.Close()
	rq := tp.(*redisQueue)

	var hbCalls int32
	b.scripts[idxHeartbeat].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.AddInt32(&hbCalls, 1)
		// 返回 {1} 模拟 doing 集仍存在
		return []interface{}{int64(1)}, nil
	}
	stubAllScriptsOK(b)
	// override heartbeat after stubAll
	b.scripts[idxHeartbeat].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.AddInt32(&hbCalls, 1)
		return []interface{}{int64(1)}, nil
	}

	// 启动 manualHandler 模式不会自动 ack（heartbeat 仅覆盖到回调返回前）
	// 这里直接调用 startHeartbeat 测原语
	stop := rq.startHeartbeat(&Item{Value: []byte("h1")})
	if stop == nil {
		t.Fatal("startHeartbeat returned nil for enabled config")
	}
	// 等 ~350ms，应至少触发 3 次心跳
	time.Sleep(350 * time.Millisecond)
	stop()

	if c := atomic.LoadInt32(&hbCalls); c < 2 {
		t.Fatalf("want >=2 heartbeats, got %d", c)
	}
}

// TestHeartbeat_StopsWhenItemRemoved 当 doing 集中已不存在时心跳自动结束
func TestHeartbeat_StopsWhenItemRemoved(t *testing.T) {
	b := &fakeScriptBuilder{}
	tp := NewRedisTopicQueue(context.Background(), "ht-stop",
		WithRedisScriptBuilder(b),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(3*time.Second),
		WithHeartbeatInterval(50*time.Millisecond),
	)
	defer tp.Close()
	rq := tp.(*redisQueue)

	stubAllScriptsOK(b)
	var hbCalls int32
	b.scripts[idxHeartbeat].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		n := atomic.AddInt32(&hbCalls, 1)
		if n >= 2 {
			// 第 2 次返回 {0} 表示 item 已不在 doing 集
			return []interface{}{int64(0)}, nil
		}
		return []interface{}{int64(1)}, nil
	}

	stop := rq.startHeartbeat(&Item{Value: []byte("h2")})
	// 等足够时间让 goroutine 自我退出
	time.Sleep(500 * time.Millisecond)
	stop() // 应已经退出，stop() 不阻塞

	c := atomic.LoadInt32(&hbCalls)
	if c < 2 {
		t.Fatalf("want >=2 calls before auto stop, got %d", c)
	}
	// 不应继续超过几次
	time.Sleep(200 * time.Millisecond)
	if cAfter := atomic.LoadInt32(&hbCalls); cAfter > c+1 {
		t.Fatalf("heartbeat continued after item removed: %d -> %d", c, cAfter)
	}
}

// TestHeartbeat_StopsOnError 心跳 error 时计数 + 继续重试（不退出）
func TestHeartbeat_ContinuesOnError(t *testing.T) {
	b := &fakeScriptBuilder{}
	fl := &fakeLogger{}
	tp := NewRedisTopicQueue(context.Background(), "ht-err",
		WithRedisScriptBuilder(b),
		WithLogger(fl),
		WithVisibilityTimeout(3*time.Second),
		WithHeartbeatInterval(50*time.Millisecond),
	)
	defer tp.Close()
	rq := tp.(*redisQueue)

	stubAllScriptsOK(b)
	var hbCalls int32
	b.scripts[idxHeartbeat].evalShaFn = func(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
		atomic.AddInt32(&hbCalls, 1)
		return nil, errBoom
	}
	b.scripts[idxHeartbeat].evalFn = b.scripts[idxHeartbeat].evalShaFn

	stop := rq.startHeartbeat(&Item{Value: []byte("h3")})
	time.Sleep(200 * time.Millisecond)
	stop()

	if c := atomic.LoadInt32(&hbCalls); c < 2 {
		t.Fatalf("want >=2 attempts, got %d", c)
	}
}

// idxHeartbeat 心跳脚本在 newRedisTopicQueue 中的注册顺序索引
const idxHeartbeat = 8 // move=0, add=1, length=2, ackSuccess=3, ackFailed=4, failedCount=5, get=6, cancel=7, heartbeat=8

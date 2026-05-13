package delayq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandwich-go/redisson"
)

// redissonScriptBuilder 把 redisson.Cmdable 适配成 delayq.RedisScriptBuilder
type redissonScriptBuilder struct{ c redisson.Cmdable }
type redissonScript struct{ s redisson.Scripter }

func (b redissonScriptBuilder) Build(src string) RedisScript {
	return redissonScript{s: b.c.CreateScript(src)}
}

func (s redissonScript) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.EvalSha(ctx, keys, args...).Slice()
}
func (s redissonScript) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.Eval(ctx, keys, args...).Slice()
}

// newTestRedisClient 使用 redisson 内置的 WithT mock 启动 miniredis
// WithDevelopment(false) 关闭开发模式校验，避免 miniredis 不支持的 CLIENT TRACKING 等命令产生噪音
func newTestRedisClient(t *testing.T) redisson.Cmdable {
	t.Helper()
	return redisson.MustNewClient(redisson.NewConf(
		redisson.WithT(t),
		redisson.WithDevelopment(false),
	))
}

func newTestBuilder(t *testing.T) RedisScriptBuilder {
	return redissonScriptBuilder{c: newTestRedisClient(t)}
}

// TestRedisQueue_PushLengthAckSuccess 覆盖 push/length/ackSuccess 基本流程
func TestRedisQueue_PushLengthAckSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp := NewRedisTopicQueue(ctx, "t1", WithRedisScriptBuilder(newTestBuilder(t)))

	item := &Item{DelaySecond: 1, Value: []byte("v1")}
	if err := tp.Push(item); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 1 {
		t.Fatalf("length want=1 got=%d", l)
	}
	rq := tp.(*redisQueue)
	if err := rq.onSuccess(item); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 0 {
		t.Fatalf("length want=0 got=%d", l)
	}
}

// TestRedisQueue_PollAndExecute 覆盖 poll 拉起 + handler 成功 ack
func TestRedisQueue_PollAndExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp := NewRedisTopicQueue(ctx, "t2", WithRedisScriptBuilder(newTestBuilder(t)))

	var got int32
	var wg sync.WaitGroup
	wg.Add(1)
	if err := tp.Start(func(item *Item) error {
		if string(item.GetValue()) == "hello" {
			atomic.AddInt32(&got, 1)
			wg.Done()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer tp.Close()

	if err := tp.Push(&Item{Value: []byte("hello")}); err != nil {
		t.Fatal(err)
	}

	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for handler, got=%d", atomic.LoadInt32(&got))
	}

	// 成功后应该被 ack 清除
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if tp.Length() == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("length should be 0 after success ack, got=%d", tp.Length())
}

// TestRedisQueue_RetryAndDeadLetter 覆盖失败重试 + 死信
func TestRedisQueue_RetryAndDeadLetter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var deadCh = make(chan *Item, 1)
	tp := NewRedisTopicQueue(ctx, "t3",
		WithRedisScriptBuilder(newTestBuilder(t)),
		WithRetryTimes(2),
		WithOnDeadLetter(func(item *Item) { deadCh <- item }),
	)

	var handled int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&handled, 1)
		return fmt.Errorf("always fail")
	}); err != nil {
		t.Fatal(err)
	}
	defer tp.Close()

	if err := tp.Push(&Item{Value: []byte("boom")}); err != nil {
		t.Fatal(err)
	}

	select {
	case di := <-deadCh:
		if string(di.GetValue()) != "boom" {
			t.Fatalf("unexpected dead letter value=%s", di.GetValue())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for dead letter, handled=%d", atomic.LoadInt32(&handled))
	}

	// 死信后应 ack 清除
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if tp.Length() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if l := tp.Length(); l != 0 {
		t.Fatalf("dead letter not cleaned, length=%d", l)
	}
	if h := atomic.LoadInt32(&handled); h < 1 {
		t.Fatalf("handler should be called at least once, got=%d", h)
	}
}

// TestRedisQueue_Reclaim 覆盖 doing 被崩溃残留时的回收
// 构造一个 doing score 已过期的场景，reclaim 应把它搬回 delay 集
func TestRedisQueue_Reclaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp := NewRedisTopicQueue(ctx, "t4", WithRedisScriptBuilder(newTestBuilder(t)))
	rq := tp.(*redisQueue)

	// 直接往 doing 集放一个 score 为过去时间的 item，模拟崩溃残留
	// 借用 addScript 把 value 写入 doing 集（addScript 语义就是 ZADD）
	expiredScore := unix() - 10
	if _, err := rq.runScript(ctx, rq.addScript, []string{rq.doingSetKey}, []byte("recl"), expiredScore); err != nil {
		t.Fatal(err)
	}

	// 调用 reclaim 把它搬回 delay 集
	if err := rq.reclaim(); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 1 {
		t.Fatalf("after reclaim length want=1 got=%d", l)
	}
}

// TestRedisQueue_Close 覆盖关闭流程
func TestRedisQueue_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp := NewRedisTopicQueue(ctx, "t5", WithRedisScriptBuilder(newTestBuilder(t)))
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tp.Close(); err == nil {
		t.Fatal("second close should return error")
	}
}

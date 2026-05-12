package delayq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryQueue_Close(t *testing.T) {
	var ctx, cancel = context.WithCancel(context.Background())
	tp := NewMemoryTopicQueue(ctx, "")

	for _, v := range []struct {
		do          func() error
		shouldError bool
	}{
		{do: func() error { return tp.Close() }, shouldError: true},
		{do: func() error { return tp.Start(nil) }, shouldError: false},
		{do: func() error { return tp.Close() }, shouldError: false},
		{do: func() error { return tp.Close() }, shouldError: true},
		{do: func() error { return tp.Start(nil) }, shouldError: false},
		{do: func() error {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-ctx.Done()
				time.Sleep(100 * time.Millisecond)
			}()
			cancel()
			wg.Wait()
			return nil
		}, shouldError: false},
		{do: func() error { return tp.Close() }, shouldError: true},
	} {
		err := v.do()
		if v.shouldError {
			if err == nil {
				t.Fatal("err should not be nil")
			}
		} else {
			if err != nil {
				t.Fatal("err should be nil")
			}
		}
	}
}

func TestMemoryQueue(t *testing.T) {
	var wg sync.WaitGroup
	var now = time.Now()
	var count int64
	tp := NewMemoryTopicQueue(context.Background(), "")
	defer tp.Close()
	err := tp.Start(func(item *Item) error {
		t.Log(time.Now().Sub(now))
		atomic.AddInt64(&count, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatal("queue start error", err)
	}
	time.Sleep(10 * time.Millisecond)

	wg.Add(1)
	if err = tp.Push(&Item{DelaySecond: int64(1)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Add(1)
	if err = tp.Push(&Item{DelaySecond: int64(2)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Wait()
	if c := atomic.LoadInt64(&count); c != 2 {
		t.Fatal("count should = 2, now is", c)
	}
}

func TestMemoryQueueHandleFailed(t *testing.T) {
	var wg sync.WaitGroup
	var now = time.Now()
	var count int64
	tp := NewMemoryTopicQueue(context.Background(), "", WithRetryTimes(2), WithOnDeadLetter(func(item *Item) {
		t.Log("got dead letter, ", item)
		wg.Done()
	}))
	defer tp.Close()
	err := tp.Start(func(item *Item) error {
		t.Log(time.Now().Sub(now))
		atomic.AddInt64(&count, 1)
		wg.Done()
		return fmt.Errorf("error")
	})
	if err != nil {
		t.Fatal("queue start error", err)
	}
	time.Sleep(10 * time.Millisecond)
	// RetryTimes=2 => 首次执行(失败) + 2 次重试(都失败) + 死信 = 4 次 wg.Done
	// 其中 handler 执行 3 次
	wg.Add(4)
	if err = tp.Push(&Item{DelaySecond: int64(1)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Wait()
	if c := atomic.LoadInt64(&count); c != 3 {
		t.Fatal("count should = 3, now is", c)
	}
}

// TestMemoryQueue_Concurrency 验证时间轮在高并发 push 下的正确性
func TestMemoryQueue_Concurrency(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "concurrent", WithMaxConcurrency(32))
	defer tp.Close()

	var done int64
	const N = 500
	var wg sync.WaitGroup
	wg.Add(N)
	err := tp.Start(func(item *Item) error {
		atomic.AddInt64(&done, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	var pushWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		pushWG.Add(1)
		go func() {
			defer pushWG.Done()
			for j := 0; j < N/10; j++ {
				if e := tp.Push(&Item{DelaySecond: int64(j%3 + 1)}); e != nil {
					t.Error(e)
					return
				}
			}
		}()
	}
	pushWG.Wait()

	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout, done=%d/%d", atomic.LoadInt64(&done), N)
	}
	if tp.Length() != 0 {
		t.Fatalf("length should be 0, got %d", tp.Length())
	}
}

// TestMemoryQueue_CloseWaitsInflight 验证 Close 等待在途 handler 返回
func TestMemoryQueue_CloseWaitsInflight(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "inflight")
	var inflight int32
	var maxSeen int32
	err := tp.Start(func(item *Item) error {
		n := atomic.AddInt32(&inflight, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if n <= m || atomic.CompareAndSwapInt32(&maxSeen, m, n) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if e := tp.Push(&Item{DelaySecond: 1}); e != nil {
			t.Fatal(e)
		}
	}
	// 等到有 inflight 任务（第 2 秒 tick 时 handler 开始并 sleep 200ms）
	// 200ms 内 inflight>0，此时调用 Close 必须等待归零
	time.Sleep(2100 * time.Millisecond)
	if e := tp.Close(); e != nil {
		t.Fatal(e)
	}
	if v := atomic.LoadInt32(&inflight); v != 0 {
		t.Fatalf("close did not wait inflight, remaining=%d", v)
	}
	if atomic.LoadInt32(&maxSeen) == 0 {
		t.Fatal("no handler ever ran")
	}
}

func BenchmarkItem(b *testing.B) {
	var wg sync.WaitGroup
	tp := NewMemoryTopicQueue(context.Background(), "")
	err := tp.Start(func(item *Item) error {
		wg.Done()
		return nil
	})
	if err != nil {
		b.Fatal("queue start error", err)
	}
	defer tp.Close()
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		err = tp.Push(&Item{DelaySecond: int64(i % 3)})
		if err != nil {
			b.Fatal("queue push item error", err)
		}
	}
	wg.Wait()
}

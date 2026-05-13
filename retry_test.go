package delayq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestComputeRetryDelay_FixedInterval 不退避时返回固定值
func TestComputeRetryDelay_FixedInterval(t *testing.T) {
	opts := newConfig(WithRetryInterval(2 * time.Second))
	for i := 1; i <= 5; i++ {
		if d := computeRetryDelay(opts, i); d != 2*time.Second {
			t.Fatalf("failedCount=%d want=2s got=%v", i, d)
		}
	}
}

// TestComputeRetryDelay_ExponentialBackoff 指数退避计算
func TestComputeRetryDelay_ExponentialBackoff(t *testing.T) {
	opts := newConfig(
		WithRetryInterval(1*time.Second),
		WithRetryBackoff(2.0),
		WithMaxRetryInterval(60*time.Second),
	)
	cases := []struct {
		failed int
		want   time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{7, 60 * time.Second}, // 1*2^6=64, 被 max 截断
		{20, 60 * time.Second},
	}
	for _, c := range cases {
		if d := computeRetryDelay(opts, c.failed); d != c.want {
			t.Errorf("failedCount=%d want=%v got=%v", c.failed, c.want, d)
		}
	}
}

// TestComputeRetryDelay_CustomFunc 自定义函数优先级最高
func TestComputeRetryDelay_CustomFunc(t *testing.T) {
	opts := newConfig(
		WithRetryInterval(1*time.Second),
		WithRetryBackoff(2.0),
		WithRetryIntervalFunc(func(failedCount int) time.Duration {
			return time.Duration(failedCount) * 100 * time.Millisecond
		}),
	)
	if d := computeRetryDelay(opts, 3); d != 300*time.Millisecond {
		t.Fatalf("custom func not used, got %v", d)
	}
}

// TestComputeRetryDelay_NegativeCustomReturn 自定义函数返回负值会被夹到 0
func TestComputeRetryDelay_NegativeCustomReturn(t *testing.T) {
	opts := newConfig(WithRetryIntervalFunc(func(int) time.Duration { return -1 * time.Second }))
	if d := computeRetryDelay(opts, 1); d != 0 {
		t.Fatalf("want 0 got %v", d)
	}
}

// TestComputeRetryDelay_FailedCountClamp failed<1 会按 1 处理
func TestComputeRetryDelay_FailedCountClamp(t *testing.T) {
	opts := newConfig(WithRetryInterval(1 * time.Second))
	if d := computeRetryDelay(opts, 0); d != time.Second {
		t.Fatalf("want 1s got %v", d)
	}
	if d := computeRetryDelay(opts, -5); d != time.Second {
		t.Fatalf("want 1s got %v", d)
	}
}

// TestComputeRetryDelay_NegativeBase 基础间隔为负也被夹到 0
func TestComputeRetryDelay_NegativeBase(t *testing.T) {
	opts := newConfig(WithRetryInterval(-1 * time.Second))
	if d := computeRetryDelay(opts, 1); d != 0 {
		t.Fatalf("want 0 got %v", d)
	}
}

// TestComputeRetryDelay_NoMaxLimit 当 MaxRetryInterval=0 时不做上限限制
func TestComputeRetryDelay_NoMaxLimit(t *testing.T) {
	opts := newConfig(
		WithRetryInterval(1*time.Second),
		WithRetryBackoff(2.0),
		WithMaxRetryInterval(0),
	)
	if d := computeRetryDelay(opts, 4); d != 8*time.Second {
		t.Fatalf("want 8s got %v", d)
	}
}

// TestMemq_RetryUsesBackoff 验证内存队列 onFailed 用了退避配置
// 重试 2 次，每次延迟 2s，因此总耗时 >= 4s
func TestMemq_RetryUsesBackoff(t *testing.T) {
	deadCh := make(chan time.Time, 1)
	tp := NewMemoryTopicQueue(context.Background(), "memq-backoff",
		WithRetryTimes(2),
		WithRetryInterval(2*time.Second),
		WithRetryBackoff(1.0), // 固定 2s
		WithOnDeadLetter(func(item *Item) { deadCh <- time.Now() }),
	)
	defer tp.Close()

	var attempts int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&attempts, 1)
		return errBoom
	}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	select {
	case end := <-deadCh:
		elapsed := end.Sub(start)
		// 第 1 次执行耗时 ~1s（push delay），失败后 +2s 重试，再失败 +2s 重试，再失败 -> 死信
		// 总计至少 ~5s
		if elapsed < 4*time.Second {
			t.Fatalf("retry too fast, elapsed=%v", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout, attempts=%d", attempts)
	}
}

// TestMemq_RetryWithCustomFunc 验证自定义重试函数被调用
func TestMemq_RetryWithCustomFunc(t *testing.T) {
	deadCh := make(chan struct{}, 1)
	var observedFailedCounts []int
	tp := NewMemoryTopicQueue(context.Background(), "memq-custom",
		WithRetryTimes(3),
		WithRetryIntervalFunc(func(failedCount int) time.Duration {
			observedFailedCounts = append(observedFailedCounts, failedCount)
			return 1 * time.Second
		}),
		WithOnDeadLetter(func(item *Item) {
			select {
			case deadCh <- struct{}{}:
			default:
			}
		}),
	)
	defer tp.Close()

	if err := tp.Start(func(item *Item) error { return errBoom }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deadCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting dead letter")
	}
	// retry func 应该被调用 3 次（每次失败一次），分别传入 1,2,3
	if len(observedFailedCounts) != 3 {
		t.Fatalf("want 3 retry calls, got %d: %v", len(observedFailedCounts), observedFailedCounts)
	}
	for i, n := range observedFailedCounts {
		if n != i+1 {
			t.Fatalf("retry call #%d expected failedCount=%d got=%d", i, i+1, n)
		}
	}
}

// errBoom 共享的失败 error
var errBoom = simpleErr("boom")

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

package delayq

import (
	"context"
	"testing"
	"time"
)

// FuzzPush 验证任意 topic/value/delay/priority 组合都不会 panic 或 race
//
// 运行：
//
//	go test -fuzz=FuzzPush -fuzztime=10s
func FuzzPush(f *testing.F) {
	f.Add("topic", []byte("v"), int64(0), int32(0))
	f.Add("", []byte{}, int64(-1), int32(-100))
	f.Add("a/b", []byte{0xFF, 0x00, 0x7F}, int64(7200), int32(1<<30))
	f.Add("中文", []byte("\x00\xff"), int64(1<<31), int32(0))

	f.Fuzz(func(t *testing.T, topic string, value []byte, delay int64, priority int32) {
		// 限制 delay 避免极大值溢出 wheel 计算
		if delay > 1<<30 {
			delay = 1 << 30
		}
		if delay < -1<<30 {
			delay = -1 << 30
		}
		tp := NewMemoryTopicQueue(context.Background(), "fuzz", WithLogger(NopLogger()))
		defer tp.Close()
		if err := tp.Start(noopHandler); err != nil {
			t.Fatal(err)
		}
		// 入队任何输入都不应 panic
		_ = tp.Push(&Item{
			Topic:       topic,
			DelaySecond: delay,
			Value:       value,
			Priority:    priority,
		})
		// Get/Cancel 接受任意 value
		_, _, _ = tp.Get(value)
		_, _ = tp.Cancel(value)
	})
}

// FuzzPushBatch 验证批量 push 输入鲁棒性
func FuzzPushBatch(f *testing.F) {
	f.Add(int64(0), int32(0), uint8(1))
	f.Add(int64(3600), int32(100), uint8(50))
	f.Add(int64(-1), int32(-1), uint8(0))

	f.Fuzz(func(t *testing.T, baseDelay int64, priority int32, count uint8) {
		if baseDelay > 1<<30 {
			baseDelay = 1 << 30
		}
		if baseDelay < -1<<30 {
			baseDelay = -1 << 30
		}
		tp := NewMemoryTopicQueue(context.Background(), "fuzzb", WithLogger(NopLogger()))
		defer tp.Close()
		if err := tp.Start(noopHandler); err != nil {
			t.Fatal(err)
		}
		items := make([]*Item, count)
		for i := range items {
			items[i] = &Item{
				DelaySecond: baseDelay + int64(i),
				Priority:    priority,
				Value:       []byte{byte(i)},
			}
		}
		_ = tp.PushBatch(items)
	})
}

// FuzzParseFloat64 / parseInt64 稳健性
func FuzzParseInt64(f *testing.F) {
	f.Add("0")
	f.Add("-1")
	f.Add("9223372036854775807")
	f.Add("not-a-number")
	f.Add("")
	f.Fuzz(func(_ *testing.T, s string) {
		_ = parseInt64(s)
		_ = parseFloat64(s)
	})
}

// FuzzComputeRetryDelay 验证退避计算不 panic 不返回 NaN
func FuzzComputeRetryDelay(f *testing.F) {
	f.Add(int64(time.Second), 1.0, int64(time.Minute), 1)
	f.Add(int64(0), 0.0, int64(0), 0)
	f.Add(int64(-1), -1.0, int64(-1), -1)
	f.Add(int64(1), 1e30, int64(time.Hour), 100)
	f.Fuzz(func(t *testing.T, baseNs int64, backoff float64, maxNs int64, failed int) {
		opts := newConfig(
			WithRetryInterval(time.Duration(baseNs)),
			WithRetryBackoff(backoff),
			WithMaxRetryInterval(time.Duration(maxNs)),
		)
		d := computeRetryDelay(opts, failed)
		if d < 0 {
			t.Fatalf("delay should not be negative: %v", d)
		}
	})
}

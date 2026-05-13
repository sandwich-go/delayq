package delayq

import (
	"testing"
	"time"
)

func TestHighThroughputPreset(t *testing.T) {
	opts := newConfig(HighThroughputPreset()...)
	if !opts.GetDisableValueIndex() {
		t.Error("HighThroughput should disable value index")
	}
	if opts.GetMaxConcurrency() != 1024 {
		t.Errorf("MaxConcurrency want=1024 got=%d", opts.GetMaxConcurrency())
	}
	if opts.GetRetryBackoff() != 2.0 {
		t.Errorf("RetryBackoff want=2.0 got=%v", opts.GetRetryBackoff())
	}
}

func TestLowLatencyPreset(t *testing.T) {
	opts := newConfig(LowLatencyPreset()...)
	if opts.GetRetryInterval() != 500*time.Millisecond {
		t.Errorf("RetryInterval want=500ms got=%v", opts.GetRetryInterval())
	}
	if opts.GetVisibilityTimeout() != 30*time.Second {
		t.Errorf("VisibilityTimeout want=30s got=%v", opts.GetVisibilityTimeout())
	}
}

func TestReliablePreset(t *testing.T) {
	opts := newConfig(ReliablePreset()...)
	if opts.GetRetryTimes() != 15 {
		t.Errorf("RetryTimes want=15 got=%d", opts.GetRetryTimes())
	}
	if opts.GetVisibilityTimeout() != 15*time.Minute {
		t.Errorf("VisibilityTimeout want=15min got=%v", opts.GetVisibilityTimeout())
	}
}

// TestPreset_Combinable 验证 Preset 与额外 Option 可以组合，且后者覆盖前者
func TestPreset_Combinable(t *testing.T) {
	// 用 HighThroughput 但覆盖 MaxConcurrency
	all := append(HighThroughputPreset(), WithMaxConcurrency(42))
	opts := newConfig(all...)
	if opts.GetMaxConcurrency() != 42 {
		t.Errorf("override failed, got %d", opts.GetMaxConcurrency())
	}
	// 其它 preset 配置仍生效
	if !opts.GetDisableValueIndex() {
		t.Error("preset DisableValueIndex should still apply")
	}
}

// TestPreset_New_Integration 验证 preset 直接传给 New 可正常使用
func TestPreset_New_Integration(t *testing.T) {
	q := New(append(LowLatencyPreset(), WithLogger(NopLogger()), WithName("test"))...)
	defer q.Close()
	if err := q.Start("t", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "t", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
}

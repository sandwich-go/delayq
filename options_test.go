package delayq

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestOptions_Defaults(t *testing.T) {
	opts := newConfig()
	if opts.GetName() != "delayq" {
		t.Fatalf("default name wrong: %s", opts.GetName())
	}
	if opts.GetPrefix() != "__dq" {
		t.Fatalf("default prefix wrong: %s", opts.GetPrefix())
	}
	if opts.GetRetryTimes() != 10 {
		t.Fatalf("default retry times wrong: %d", opts.GetRetryTimes())
	}
	if opts.GetMaxConcurrency() != 256 {
		t.Fatalf("default concurrency wrong: %d", opts.GetMaxConcurrency())
	}
	if opts.GetRedisScriptBuilder() != nil {
		t.Fatalf("default builder should be nil")
	}
	if opts.GetOnDeadLetter() != nil {
		t.Fatalf("default dead letter should be nil")
	}
	if opts.GetMonitorCounter() == nil {
		t.Fatalf("default monitor counter should be non-nil no-op")
	}
	if opts.GetLogger() != nil {
		t.Fatalf("default logger should be nil (injected by baseQueue)")
	}
	if opts.GetVisibilityTimeout() != 10*time.Minute {
		t.Fatalf("default visibility timeout wrong: %v", opts.GetVisibilityTimeout())
	}
	if opts.GetRetryInterval() != time.Second {
		t.Fatalf("default retry interval wrong: %v", opts.GetRetryInterval())
	}
	if opts.GetRetryBackoff() != 1.0 {
		t.Fatalf("default backoff wrong: %v", opts.GetRetryBackoff())
	}
	if opts.GetMaxRetryInterval() != 60*time.Second {
		t.Fatalf("default max retry interval wrong: %v", opts.GetMaxRetryInterval())
	}
	if opts.GetRetryIntervalFunc() != nil {
		t.Fatalf("default retry interval func should be nil")
	}
}

func TestOptions_Apply(t *testing.T) {
	opts := newConfig(
		WithName("n"),
		WithPrefix("p"),
		WithRetryTimes(3),
		WithMaxConcurrency(16),
		WithOnDeadLetter(func(i *Item) {}),
		WithMonitorCounter(func(m string, v int64, l prometheus.Labels) {}),
		WithLogger(NopLogger()),
		WithVisibilityTimeout(2*time.Minute),
		WithRetryInterval(500*time.Millisecond),
		WithRetryBackoff(2.5),
		WithMaxRetryInterval(2*time.Minute),
		WithRetryIntervalFunc(func(int) time.Duration { return 0 }),
	)
	if opts.GetName() != "n" || opts.GetPrefix() != "p" || opts.GetRetryTimes() != 3 ||
		opts.GetMaxConcurrency() != 16 {
		t.Fatal("values not applied")
	}
	if opts.GetOnDeadLetter() == nil || opts.GetMonitorCounter() == nil || opts.GetLogger() == nil {
		t.Fatal("callbacks/logger not set")
	}
	if opts.GetVisibilityTimeout() != 2*time.Minute {
		t.Fatalf("visibility timeout not applied: %v", opts.GetVisibilityTimeout())
	}
	if opts.GetRetryInterval() != 500*time.Millisecond {
		t.Fatalf("retry interval not applied: %v", opts.GetRetryInterval())
	}
	if opts.GetRetryBackoff() != 2.5 {
		t.Fatalf("retry backoff not applied: %v", opts.GetRetryBackoff())
	}
	if opts.GetMaxRetryInterval() != 2*time.Minute {
		t.Fatalf("max retry interval not applied: %v", opts.GetMaxRetryInterval())
	}
	if opts.GetRetryIntervalFunc() == nil {
		t.Fatalf("retry interval func not applied")
	}

	// ApplyOption 后续再覆盖
	opts.ApplyOption(WithName("nn"))
	if opts.GetName() != "nn" {
		t.Fatalf("apply failed, got %s", opts.GetName())
	}
}

func TestOptions_WatchDog(t *testing.T) {
	called := 0
	InstallOptionsWatchDog(func(cc *Options) { called++ })
	defer InstallOptionsWatchDog(nil)
	_ = newConfig()
	if called != 1 {
		t.Fatalf("watchdog should be invoked once, got %d", called)
	}
}

func TestOptions_OptionsInterface(t *testing.T) {
	var _ OptionsInterface = newConfig()
	var _ OptionsVisitor = newConfig()
}

// TestOptions_Declaration 验证 option 声明函数可被调用
func TestOptions_Declaration(t *testing.T) {
	m := OptionsOptionDeclareWithDefault()
	if m == nil {
		t.Fatal("declaration should not be nil")
	}
}

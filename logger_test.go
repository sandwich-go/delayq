package delayq

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
)

// TestDefaultLogger_Levels 验证默认 logger 按级别打印并输出到目标 writer
func TestDefaultLogger_Levels(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &defaultLogger{l: log.New(buf, "[delayq] ", 0)}
	l.Debugf("d=%d", 1)
	l.Infof("i=%d", 2)
	l.Warnf("w=%d", 3)
	l.Errorf("e=%d", 4)
	out := buf.String()
	for _, want := range []string{"DEBUG d=1", "INFO  i=2", "WARN  w=3", "ERROR e=4", "[delayq]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q, got:\n%s", want, out)
		}
	}
}

// TestDefaultLogger_Constructor 验证构造函数返回非 nil 且可用
func TestDefaultLogger_Constructor(t *testing.T) {
	l := newDefaultLogger()
	if l == nil {
		t.Fatal("logger should not be nil")
	}
	// 不应 panic
	l.Debugf("x")
	l.Infof("x")
	l.Warnf("x")
	l.Errorf("x")
}

// TestNopLogger 验证 NopLogger 丢弃全部日志，不 panic
func TestNopLogger(t *testing.T) {
	l := NopLogger()
	if l == nil {
		t.Fatal("NopLogger should not be nil")
	}
	l.Debugf("x %d", 1)
	l.Infof("x %d", 2)
	l.Warnf("x %d", 3)
	l.Errorf("x %d", 4)
}

// fakeLogger 并发安全的内存 logger，用于测试注入
type fakeLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (f *fakeLogger) Debugf(format string, args ...interface{}) { f.writef("D "+format, args...) }
func (f *fakeLogger) Infof(format string, args ...interface{})  { f.writef("I "+format, args...) }
func (f *fakeLogger) Warnf(format string, args ...interface{})  { f.writef("W "+format, args...) }
func (f *fakeLogger) Errorf(format string, args ...interface{}) { f.writef("E "+format, args...) }
func (f *fakeLogger) writef(format string, args ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buf.WriteString(strings.TrimRight(stringsSprintf(format, args...), "\n") + "\n")
}
func (f *fakeLogger) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.String()
}

// 避免引入 fmt 时报循环（实际直接用 fmt.Sprintf 也可，这里保持简单）
func stringsSprintf(format string, args ...interface{}) string {
	// 使用标准 fmt；为保持依赖最小就地 import 模拟
	return fmtSprintf(format, args...)
}

// TestCustomLogger_WiredIntoQueue 验证 WithLogger 注入后业务 handler panic/错误会走自定义 logger
func TestCustomLogger_WiredIntoQueue(t *testing.T) {
	fl := &fakeLogger{}
	q := NewMemoryTopicQueue(testCtx(), "log-topic", WithLogger(fl), WithRetryTimes(0))
	defer q.Close()
	doneCh := make(chan struct{}, 1)
	if err := q.Start(func(item *Item) error {
		defer func() {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		}()
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	// 等 dead letter 日志/recover 写入
	waitUntil(t, 3*1000, func() bool {
		return strings.Contains(fl.String(), "handle panic") || strings.Contains(fl.String(), "dead letter")
	})
	if s := fl.String(); !strings.Contains(s, "handle panic") {
		t.Fatalf("logger should capture panic, got:\n%s", s)
	}
}

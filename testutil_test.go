package delayq

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// testCtx 返回 background ctx，方便在测试里调用
func testCtx() context.Context { return context.Background() }

// fmtSprintf 简单封装 fmt.Sprintf，避免 logger_test.go 额外 import
func fmtSprintf(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

// waitUntil 在 timeoutMs 毫秒内反复检查 fn，直到返回 true 或超时
func waitUntil(t *testing.T, timeoutMs int, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitUntil timeout after %dms", timeoutMs)
}

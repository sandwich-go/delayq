package delayq

import (
	"sync"
	"time"
)

// tokenBucket 简单的 token bucket 限流器（线程安全），用于 Push QPS 限流。
//
// 设计：每秒补充 ratePerSec 个 token，桶容量 burst。
// Allow() 非阻塞地尝试消费 1 个 token，成功返回 true。
type tokenBucket struct {
	mu         sync.Mutex
	ratePerSec float64
	burst      float64
	tokens     float64
	last       time.Time
}

// newTokenBucket 创建一个 token bucket。
// ratePerSec<=0 或 burst<=0 时返回 nil 表示不限流。
func newTokenBucket(ratePerSec, burst float64) *tokenBucket {
	if ratePerSec <= 0 || burst <= 0 {
		return nil
	}
	return &tokenBucket{
		ratePerSec: ratePerSec,
		burst:      burst,
		tokens:     burst,
		last:       nowFunc(),
	}
}

// Allow 非阻塞地尝试消费一个 token，成功返回 true。
func (b *tokenBucket) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := nowFunc()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.ratePerSec
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// AllowN 非阻塞地尝试消费 n 个 token，成功返回 true。
// n<=0 直接返回 true。
func (b *tokenBucket) AllowN(n int) bool {
	if b == nil || n <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := nowFunc()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.ratePerSec
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		return true
	}
	return false
}

package delayq

import (
	"math"
	"sync/atomic"
	"time"
)

var nowFunc = time.Now

func unix() int64 { return nowFunc().Unix() }

// computeRetryDelay 根据失败次数和配置计算下次重试延迟
// failedCount 从 1 开始（即第 1 次失败后的延迟）
// 优先使用 opts.RetryIntervalFunc；否则根据 RetryInterval * RetryBackoff^(failedCount-1) 计算，
// 上限受 MaxRetryInterval 约束
func computeRetryDelay(opts *Options, failedCount int) time.Duration {
	if failedCount < 1 {
		failedCount = 1
	}
	if f := opts.GetRetryIntervalFunc(); f != nil {
		d := f(failedCount)
		if d < 0 {
			d = 0
		}
		return d
	}
	base := opts.GetRetryInterval()
	if base < 0 {
		base = 0
	}
	backoff := opts.GetRetryBackoff()
	maxInterval := opts.GetMaxRetryInterval()
	if maxInterval < 0 {
		maxInterval = 0
	}
	d := base
	if backoff > 1.0 {
		mult := math.Pow(backoff, float64(failedCount-1))
		// 防止溢出
		if math.IsInf(mult, 0) || mult > float64(math.MaxInt64)/float64(base+1) {
			d = maxInterval
		} else {
			d = time.Duration(float64(base) * mult)
		}
	}
	if maxInterval > 0 && d > maxInterval {
		d = maxInterval
	}
	if d < 0 {
		d = 0
	}
	return d
}

// atomicInt32 is an atomic type-safe wrapper for int32 values.
type atomicInt32 int32

// Set Store atomically stores the passed int32.
func (i *atomicInt32) Set(n int32) { atomic.StoreInt32((*int32)(i), n) }

// Get Load atomically loads the wrapped int32.
func (i *atomicInt32) Get() int32 {
	return atomic.LoadInt32((*int32)(i))
}

// CompareAndSwap executes the compare-and-swap operation for a int32 value.
func (i *atomicInt32) CompareAndSwap(oldval, newval int32) (swapped bool) {
	return atomic.CompareAndSwapInt32((*int32)(i), oldval, newval)
}

// atomicInt64 is an atomic type-safe wrapper for int64 values.
type atomicInt64 int64

// Add atomically adds delta and returns the new value.
func (i *atomicInt64) Add(delta int64) int64 {
	return atomic.AddInt64((*int64)(i), delta)
}

// Get atomically loads the wrapped int64.
func (i *atomicInt64) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

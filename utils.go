package delayq

import (
	"sync/atomic"
	"time"
)

var nowFunc = time.Now

func unix() int64 { return nowFunc().Unix() }

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

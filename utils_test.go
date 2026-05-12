package delayq

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAtomicInt32_GetSet(t *testing.T) {
	var a atomicInt32
	if v := a.Get(); v != 0 {
		t.Fatalf("want 0 got %d", v)
	}
	a.Set(42)
	if v := a.Get(); v != 42 {
		t.Fatalf("want 42 got %d", v)
	}
}

func TestAtomicInt32_CAS(t *testing.T) {
	var a atomicInt32
	a.Set(10)
	if !a.CompareAndSwap(10, 20) {
		t.Fatal("CAS should succeed")
	}
	if v := a.Get(); v != 20 {
		t.Fatalf("want 20 got %d", v)
	}
	if a.CompareAndSwap(10, 30) {
		t.Fatal("CAS should fail when old mismatch")
	}
	if v := a.Get(); v != 20 {
		t.Fatalf("want 20 got %d", v)
	}
}

func TestAtomicInt32_Concurrent(t *testing.T) {
	var a atomicInt32
	var wg sync.WaitGroup
	const goroutines = 50
	const perG = 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				for {
					old := a.Get()
					if a.CompareAndSwap(old, old+1) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	if v := a.Get(); v != goroutines*perG {
		t.Fatalf("want %d got %d", goroutines*perG, v)
	}
}

func TestUnix_Now(t *testing.T) {
	before := time.Now().Unix()
	v := unix()
	after := time.Now().Unix()
	if v < before || v > after {
		t.Fatalf("unix() returned out of range: before=%d v=%d after=%d", before, v, after)
	}
}

func TestUnix_Injectable(t *testing.T) {
	// 验证 nowFunc 是可替换的（用于单测注入时钟）
	original := nowFunc
	defer func() { nowFunc = original }()
	nowFunc = func() time.Time { return time.Unix(1700000000, 0) }
	if v := unix(); v != 1700000000 {
		t.Fatalf("want 1700000000 got %d", v)
	}
}

// TestAtomicInt32_GoStdCompat 保证我们自定义的 atomicInt32 与 std/atomic 行为一致
func TestAtomicInt32_GoStdCompat(t *testing.T) {
	var a atomicInt32
	var s int32
	for i := 0; i < 100; i++ {
		a.Set(int32(i))
		atomic.StoreInt32(&s, int32(i))
		if a.Get() != atomic.LoadInt32(&s) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

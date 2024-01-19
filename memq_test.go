package delayq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueue_Close(t *testing.T) {
	var ctx, cancel = context.WithCancel(context.Background())
	tp := NewMemoryTopicQueue(ctx, "")

	for _, v := range []struct {
		do          func() error
		shouldError bool
	}{
		{do: func() error { return tp.Close() }, shouldError: true},
		{do: func() error { return tp.Start(nil) }, shouldError: false}, {do: func() error { return tp.Close() }, shouldError: false},
		{do: func() error { return tp.Close() }, shouldError: true},
		{do: func() error { return tp.Start(nil) }, shouldError: false},
		{do: func() error {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-ctx.Done():
					time.Sleep(1 * time.Second)
					return
				}
			}()
			cancel()
			wg.Wait()
			return nil
		}, shouldError: false},
		{do: func() error { return tp.Close() }, shouldError: true},
	} {
		err := v.do()
		if v.shouldError {
			if err == nil {
				t.Fatal("err should not be nil")
			}
		} else {
			if err != nil {
				t.Fatal("err should be nil")
			}
		}
	}
}

func TestMemoryQueue(t *testing.T) {
	var wg sync.WaitGroup
	var now = time.Now()
	var count = 0
	tp := NewMemoryTopicQueue(context.Background(), "")
	err := tp.Start(func(item *Item) error {
		t.Log(time.Now().Sub(now))
		wg.Done()
		count++
		return nil
	})
	if err != nil {
		t.Fatal("queue start error", err)
	}
	time.Sleep(10 * time.Millisecond)

	wg.Add(1)
	if err = tp.Push(&Item{DelaySecond: int64(1)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Add(1)
	if err = tp.Push(&Item{DelaySecond: int64(2)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Wait()
	if count != 2 {
		t.Fatal("count should = 2, now is", count)
	}
}

func TestMemoryQueueHandleFailed(t *testing.T) {
	var wg sync.WaitGroup
	var now = time.Now()
	var count = 0
	tp := NewMemoryTopicQueue(context.Background(), "", WithRetryTimes(2), WithOnDeadLetter(func(item *Item) {
		t.Log("got dead letter, ", item)
		wg.Done()
	}))
	err := tp.Start(func(item *Item) error {
		t.Log(time.Now().Sub(now))
		count++
		wg.Done()
		return fmt.Errorf("error")
	})
	if err != nil {
		t.Fatal("queue start error", err)
	}
	time.Sleep(10 * time.Millisecond)
	wg.Add(4)
	if err = tp.Push(&Item{DelaySecond: int64(1)}); err != nil {
		t.Fatal("queue push item error", err)
	}
	wg.Wait()
	if count != 3 {
		t.Fatal("count should = 3, now is", count)
	}
}

func BenchmarkItem(b *testing.B) {
	var wg sync.WaitGroup
	tp := NewMemoryTopicQueue(context.Background(), "")
	err := tp.Start(func(item *Item) error {
		wg.Done()
		return nil
	})
	if err != nil {
		b.Fatal("queue start error", err)
	}
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		err = tp.Push(&Item{DelaySecond: int64(i % 3)})
		if err != nil {
			b.Fatal("queue push item error", err)
		}
	}
	wg.Wait()
}

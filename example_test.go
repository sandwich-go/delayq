package delayq_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandwich-go/delayq"
)

// Example 演示最简用法：内存延迟队列、handler 自动 ack。
func Example() {
	dq := delayq.New(delayq.WithLogger(delayq.NopLogger()))
	defer dq.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	_ = dq.Start("greetings", func(item *delayq.Item) error {
		fmt.Printf("got: %s\n", item.GetValue())
		wg.Done()
		return nil
	})

	_ = dq.Push(&delayq.Item{
		Topic:       "greetings",
		DelaySecond: 1,
		Value:       []byte("hello"),
	})

	wg.Wait()
	// Output: got: hello
}

// ExampleNew_priority 演示同时间内按 priority 降序派发。
func ExampleNew_priority() {
	dq := delayq.New(
		delayq.WithLogger(delayq.NopLogger()),
		delayq.WithMaxConcurrency(1), // 单线程消费才能观察到顺序
	)
	defer dq.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	_ = dq.Start("p", func(item *delayq.Item) error {
		fmt.Println(string(item.GetValue()))
		wg.Done()
		return nil
	})

	// 同样 1s 后到期，priority 高的先派发
	_ = dq.PushBatch([]*delayq.Item{
		{Topic: "p", DelaySecond: 1, Value: []byte("low"), Priority: 1},
		{Topic: "p", DelaySecond: 1, Value: []byte("mid"), Priority: 50},
		{Topic: "p", DelaySecond: 1, Value: []byte("high"), Priority: 100},
	})

	wg.Wait()
	// Output:
	// high
	// mid
	// low
}

// ExampleQueue_Get 查询某个 value 是否在队列中以及剩余延迟。
func ExampleQueue_Get() {
	dq := delayq.New(delayq.WithLogger(delayq.NopLogger()))
	defer dq.Close()

	_ = dq.Start("orders", func(item *delayq.Item) error { return nil })
	_ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 60, Value: []byte("o-1")})

	_, exists, _ := dq.Get("orders", []byte("o-1"))
	fmt.Println("exists:", exists)
	// Output: exists: true
}

// ExampleQueue_Cancel 取消未派发的 item。
func ExampleQueue_Cancel() {
	dq := delayq.New(delayq.WithLogger(delayq.NopLogger()))
	defer dq.Close()

	_ = dq.Start("orders", func(item *delayq.Item) error { return nil })
	_ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 60, Value: []byte("o-2")})

	canceled, _ := dq.Cancel("orders", []byte("o-2"))
	fmt.Println("canceled:", canceled)
	// Output: canceled: true
}

// ExampleQueue_StartManualAck 演示手动 ack 模式：业务异步处理后显式 Ack/Nack。
func ExampleQueue_StartManualAck() {
	dq := delayq.New(delayq.WithLogger(delayq.NopLogger()))
	defer dq.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	_ = dq.StartManualAck("emails", func(item *delayq.Item, ack delayq.Acker) {
		// 模拟异步处理后 ack
		go func() {
			fmt.Printf("processing %s\n", item.GetValue())
			ack.Ack()
			wg.Done()
		}()
	})

	_ = dq.Push(&delayq.Item{Topic: "emails", DelaySecond: 1, Value: []byte("user@example.com")})
	wg.Wait()
	// Output: processing user@example.com
}

// ExampleQueue_CloseGracefully 演示优雅退出：拒绝新 Push 并等待消化完毕。
func ExampleQueue_CloseGracefully() {
	dq := delayq.New(delayq.WithLogger(delayq.NopLogger()))

	var processed int
	var mu sync.Mutex
	_ = dq.Start("jobs", func(item *delayq.Item) error {
		mu.Lock()
		processed++
		mu.Unlock()
		return nil
	})

	for i := 0; i < 3; i++ {
		_ = dq.Push(&delayq.Item{Topic: "jobs", DelaySecond: 1, Value: []byte{byte(i)}})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = dq.CloseGracefully(ctx)

	mu.Lock()
	fmt.Println("processed:", processed)
	mu.Unlock()
	// Output: processed: 3
}

// ExampleWithRetryBackoff 演示指数退避配置：失败重试 1s, 2s, 4s, 8s ...，上限 30s。
func ExampleWithRetryBackoff() {
	dq := delayq.New(
		delayq.WithLogger(delayq.NopLogger()),
		delayq.WithRetryTimes(5),
		delayq.WithRetryInterval(1*time.Second),
		delayq.WithRetryBackoff(2.0),
		delayq.WithMaxRetryInterval(30*time.Second),
	)
	defer dq.Close()
	fmt.Println("ok")
	// Output: ok
}

// ExampleHighThroughputPreset 演示使用预设（preset）+ 自定义 Option 组合。
func ExampleHighThroughputPreset() {
	dq := delayq.New(append(delayq.HighThroughputPreset(),
		delayq.WithName("myapp"),
		delayq.WithLogger(delayq.NopLogger()),
	)...)
	defer dq.Close()
	fmt.Println("ok")
	// Output: ok
}

// ExampleWithPushRatePerSec 演示 token bucket 限流。
func ExampleWithPushRatePerSec() {
	dq := delayq.New(
		delayq.WithLogger(delayq.NopLogger()),
		delayq.WithPushRatePerSec(1000), // 每秒 1000 次
		delayq.WithPushBurst(2000),      // burst 2000
	)
	defer dq.Close()
	_ = dq.Start("t", func(item *delayq.Item) error { return nil })

	err := dq.Push(&delayq.Item{Topic: "t", Value: []byte("x")})
	fmt.Println("first push err:", err)
	// Output: first push err: <nil>
}

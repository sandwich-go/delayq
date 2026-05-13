// 内存延迟队列示例：Push / 优先级 / 批量 / Get / Cancel
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandwich-go/delayq"
)

func main() {
	dq := delayq.New(
		delayq.WithMaxConcurrency(16),
		delayq.WithRetryTimes(3),
		delayq.WithRetryInterval(500*time.Millisecond),
		delayq.WithOnDeadLetter(func(item *delayq.Item) {
			fmt.Printf("[dead] %s\n", item.GetValue())
		}),
	)
	defer dq.Close()

	var wg sync.WaitGroup
	if err := dq.Start("orders", func(item *delayq.Item) error {
		fmt.Printf("[handle] topic=%s value=%s priority=%d\n",
			item.GetTopic(), item.GetValue(), item.GetPriority())
		wg.Done()
		return nil
	}); err != nil {
		panic(err)
	}

	// 普通推送
	wg.Add(2)
	_ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 1, Value: []byte("o1")})
	_ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 1, Value: []byte("o2")})

	// 优先级推送：相同到期时间，priority 高先处理
	wg.Add(3)
	_ = dq.PushBatch([]*delayq.Item{
		{Topic: "orders", DelaySecond: 2, Value: []byte("low"), Priority: 1},
		{Topic: "orders", DelaySecond: 2, Value: []byte("mid"), Priority: 50},
		{Topic: "orders", DelaySecond: 2, Value: []byte("high"), Priority: 100},
	})

	// 查询：剩余延迟
	if remain, exists, _ := dq.Get("orders", []byte("o1")); exists {
		fmt.Printf("[query] o1 还有 %v 后执行\n", remain)
	}

	// 取消：cancel-me 不会被处理
	wg.Add(0)
	_ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 3, Value: []byte("cancel-me")})
	if canceled, _ := dq.Cancel("orders", []byte("cancel-me")); canceled {
		fmt.Printf("[cancel] cancel-me 已取消\n")
	}

	// 等所有任务派发
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		fmt.Println("[timeout]")
	}

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = dq.CloseGracefully(ctx)
}

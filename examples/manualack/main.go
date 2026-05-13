// 手动 Ack 示例：handler 启动异步任务，由后台决定 Ack/Nack
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/sandwich-go/delayq"
)

func processAsync(item *delayq.Item) error {
	_ = item // 这里仅模拟，实际可用 item 中的 payload
	time.Sleep(time.Duration(50+rand.Intn(200)) * time.Millisecond)
	if rand.Intn(3) == 0 {
		return fmt.Errorf("transient failure")
	}
	return nil
}

func main() {
	var ackCnt, nackCnt int64

	dq := delayq.New(
		delayq.WithMaxConcurrency(8),
		delayq.WithRetryTimes(2),
		delayq.WithRetryInterval(500*time.Millisecond),
	)
	defer dq.Close()

	if err := dq.StartManualAck("emails", func(item *delayq.Item, ack delayq.Acker) {
		// 在新 goroutine 中处理，handler 立即返回
		go func() {
			if err := processAsync(item); err != nil {
				atomic.AddInt64(&nackCnt, 1)
				ack.Nack(err)
				return
			}
			atomic.AddInt64(&ackCnt, 1)
			ack.Ack()
		}()
	}); err != nil {
		panic(err)
	}

	// 推 20 条消息
	for i := 0; i < 20; i++ {
		_ = dq.Push(&delayq.Item{
			Topic:       "emails",
			DelaySecond: 1,
			Value:       []byte(fmt.Sprintf("user-%d", i)),
		})
	}

	// 优雅退出，等所有 ack/nack 完成
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dq.CloseGracefully(ctx); err != nil {
		fmt.Printf("[drain timeout] %v\n", err)
	}

	fmt.Printf("\n[result] ack=%d nack=%d\n", atomic.LoadInt64(&ackCnt), atomic.LoadInt64(&nackCnt))
}

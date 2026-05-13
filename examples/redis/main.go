// Redis 分布式延迟队列示例。
// 使用 sandwich-go/redisson 作为客户端。本示例默认连接 127.0.0.1:6379。
//
// 运行前请确保本地有可用 Redis，或通过 REDIS_ADDR 环境变量指定地址。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sandwich-go/delayq"
	"github.com/sandwich-go/redisson"
)

// scriptBuilder 把 redisson.Cmdable 适配成 delayq.RedisScriptBuilder
type scriptBuilder struct{ c redisson.Cmdable }

func (b scriptBuilder) Build(src string) delayq.RedisScript {
	return script{s: b.c.CreateScript(src)}
}

type script struct{ s redisson.Scripter }

func (s script) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.EvalSha(ctx, keys, args...).Slice()
}

func (s script) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return s.s.Eval(ctx, keys, args...).Slice()
}

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redisson.MustNewClient(redisson.NewConf(
		redisson.WithAddrs(addr),
		redisson.WithDevelopment(false),
	))

	dq := delayq.New(
		delayq.WithName("myapp"),
		delayq.WithRedisScriptBuilder(scriptBuilder{c: c}),
		delayq.WithVisibilityTimeout(2*time.Minute),
		delayq.WithRetryTimes(3),
		delayq.WithRetryInterval(2*time.Second),
		delayq.WithRetryBackoff(2.0),
		delayq.WithMaxRetryInterval(60*time.Second),
		delayq.WithOnDeadLetter(func(item *delayq.Item) {
			fmt.Printf("[dead] %s\n", item.GetValue())
		}),
	)
	defer dq.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	if err := dq.Start("notifications", func(item *delayq.Item) error {
		fmt.Printf("[handle] %s\n", item.GetValue())
		wg.Done()
		return nil
	}); err != nil {
		panic(err)
	}

	// 推送 3 条 1 秒后到期的消息（注意：Redis 模式下 value 不可重复）
	for i := 1; i <= 3; i++ {
		v := fmt.Sprintf("notify-%d", i)
		if err := dq.Push(&delayq.Item{
			Topic:       "notifications",
			DelaySecond: 1,
			Value:       []byte(v),
		}); err != nil {
			panic(err)
		}
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
		fmt.Println("[done]")
	case <-time.After(10 * time.Second):
		fmt.Println("[timeout]")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = dq.CloseGracefully(ctx)
}

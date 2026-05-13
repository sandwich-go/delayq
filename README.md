# delayq

Go 语言实现的延迟队列，支持：

- **内存式延迟队列**：单进程内基于时间轮（精度 1 秒）
- **分布式延迟队列**：基于 Redis ZSET，支持 visibility timeout、崩溃恢复（reclaim）、独立失败计数
- **统一接口**：内存与 Redis 实现共享同一组 API
- **可观测性**：内置 Prometheus Collector + 自定义 MonitorCounter
- **可控并发**：单 topic 业务 handler 并发数可限
- **可配置重试**：固定间隔 / 指数退避 / 自定义函数 / 死信回调

## 安装

```bash
go get github.com/sandwich-go/delayq
```

## 快速开始

```go
package main

import (
    "fmt"
    "time"

    "github.com/sandwich-go/delayq"
)

func main() {
    dq := delayq.New()
    defer dq.Close()

    if err := dq.Start("test", func(item *delayq.Item) error {
        fmt.Printf("got item: %s\n", item.GetValue())
        return nil
    }); err != nil {
        panic(err)
    }

    if err := dq.Push(&delayq.Item{
        Topic:       "test",
        DelaySecond: 1,
        Value:       []byte("hello"),
    }); err != nil {
        panic(err)
    }

    time.Sleep(2 * time.Second)
}
```

## 内存式延迟队列

未配置 `RedisScriptBuilder` 时自动使用内存实现：

```go
dq := delayq.New(
    delayq.WithMaxConcurrency(64),     // handler 并发上限
    delayq.WithRetryTimes(5),          // 最多重试 5 次
    delayq.WithRetryInterval(2*time.Second),
    delayq.WithRetryBackoff(2.0),      // 指数退避
    delayq.WithMaxRetryInterval(60*time.Second),
    delayq.WithOnDeadLetter(func(item *delayq.Item) {
        // 重试耗尽后回调
        log.Printf("dead letter: %s", item.GetValue())
    }),
)
```

## 分布式延迟队列（Redis）

通过 `RedisScriptBuilder` 注入 Redis 客户端。下面以 `github.com/sandwich-go/redisson` 为例：

```go
package main

import (
    "context"
    "fmt"

    "github.com/sandwich-go/delayq"
    "github.com/sandwich-go/redisson"
)

type scriptBuilder struct{ c redisson.Cmdable }
type script struct{ s redisson.Scripter }

func (b scriptBuilder) Build(src string) delayq.RedisScript {
    return script{s: b.c.CreateScript(src)}
}

func (s script) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
    return s.s.EvalSha(ctx, keys, args...).Slice()
}

func (s script) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
    return s.s.Eval(ctx, keys, args...).Slice()
}

func main() {
    c := redisson.MustNewClient(redisson.NewConf(redisson.WithAddrs("127.0.0.1:6379")))
    dq := delayq.New(delayq.WithRedisScriptBuilder(scriptBuilder{c}))
    defer dq.Close()

    if err := dq.Start("orders", func(item *delayq.Item) error {
        fmt.Printf("processing %s\n", item.GetValue())
        return nil
    }); err != nil {
        panic(err)
    }

    _ = dq.Push(&delayq.Item{Topic: "orders", DelaySecond: 30, Value: []byte("order#1")})
}
```

### Redis 数据结构

每个 topic 在 Redis 中使用三个 key：

| Key | 类型 | 用途 |
|-----|------|------|
| `<prefix>:do:{<topic>}` | ZSET | 延迟集，score 为预期执行时间戳（秒） |
| `<prefix>:doing:{<topic>}` | ZSET | 处理中集，score 为 `now + VisibilityTimeout` |
| `<prefix>:failed:{<topic>}` | HASH | value → 失败计数 |

`{<topic>}` 中的 `{}` 是 Redis Cluster 的 hash tag，确保同一 topic 的所有 key 落在同一 slot，从而保证 Lua 脚本可以原子地操作多个 key。

### Visibility Timeout

`Push` 后 `poll` 把 item 从 delay 集搬到 doing 集，并将其 score 设为 `now + VisibilityTimeout`。当业务 handler 在这段时间内未 ack 成功（进程崩溃、handler 阻塞），`reclaim` 任务会把它搬回 delay 集重新派发。

```go
dq := delayq.New(
    delayq.WithRedisScriptBuilder(builder),
    delayq.WithVisibilityTimeout(5*time.Minute), // 默认 10 分钟
)
```

> **不要**把 VisibilityTimeout 设得比业务 handler 最大耗时还短，否则会导致重复处理。

## 重试策略

失败重试间隔的优先级：

1. `WithRetryIntervalFunc(func(failedCount int) time.Duration)` — 完全自定义
2. `WithRetryInterval(d) + WithRetryBackoff(b) + WithMaxRetryInterval(max)` — 指数退避

```go
// 自定义函数：使用抖动避免惊群
dq := delayq.New(
    delayq.WithRetryIntervalFunc(func(failedCount int) time.Duration {
        return time.Duration(failedCount)*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
    }),
)

// 或固定指数退避：1s, 2s, 4s, 8s, ..., 上限 60s
dq := delayq.New(
    delayq.WithRetryInterval(1*time.Second),
    delayq.WithRetryBackoff(2.0),
    delayq.WithMaxRetryInterval(60*time.Second),
)
```

> 内存队列的时间轮粒度为 1 秒，重试延迟会被向上取整到秒级。

## 监控

### Prometheus Collector

```go
dq := delayq.New(delayq.WithName("myapp"))
prometheus.MustRegister(dq.Collector())
// 暴露指标 myapp_status_queue_length{queue="<topic>"}
```

### MonitorCounter

每次入队 / 处理成功 / 处理失败 / poll / reclaim 都会调用 `MonitorCounter`：

```go
dq := delayq.New(delayq.WithMonitorCounter(func(metric string, value int64, labels prometheus.Labels) {
    counter.WithLabelValues(metric, labels["Queue"]).Add(float64(value))
}))
```

可能的 metric 名：

| Metric | 触发时机 |
|--------|---------|
| `delayq_produce` | Push 成功 |
| `delayq_produce_error` | Push 失败 |
| `delayq_handle` | Handler 处理成功 |
| `delayq_handle_error` | Handler 返回 error 或 panic |
| `delayq_poll_error` | Redis poll 脚本失败 |
| `delayq_reclaim` | 一次 reclaim 搬运的 item 数 |
| `delayq_reclaim_error` | reclaim 脚本失败 |

## 日志

通过 `WithLogger` 注入自定义实现：

```go
dq := delayq.New(delayq.WithLogger(myLogger))     // 接入 zap/logrus/slog
dq := delayq.New(delayq.WithLogger(delayq.NopLogger())) // 关闭日志
```

默认输出到 stderr，前缀 `[delayq]`。

## Options 一览

| Option | 默认值 | 说明 |
|--------|--------|------|
| `WithName(string)` | `"delayq"` | Collector 指标前缀 |
| `WithPrefix(string)` | `"__dq"` | Redis key 前缀 |
| `WithRedisScriptBuilder(b)` | `nil` | 提供则启用 Redis 后端 |
| `WithRetryTimes(int)` | `10` | 失败重试次数；超过进入死信 |
| `WithOnDeadLetter(func)` | `nil` | 死信回调；未设置时仅打 WARN 日志 |
| `WithMonitorCounter(func)` | no-op | 业务监控计数函数 |
| `WithLogger(Logger)` | stderr | 日志注入；可用 `NopLogger()` 关闭 |
| `WithMaxConcurrency(int)` | `256` | 单 topic 最大并发 handler；`<=0` 不限 |
| `WithVisibilityTimeout(d)` | `10*time.Minute` | Redis 处理超时 |
| `WithRetryInterval(d)` | `1*time.Second` | 基础重试间隔 |
| `WithRetryBackoff(float64)` | `1.0` | 退避系数；`>1` 启用指数退避 |
| `WithMaxRetryInterval(d)` | `60*time.Second` | 退避上限 |
| `WithRetryIntervalFunc(func)` | `nil` | 自定义重试间隔，优先级最高 |

## 注意事项

- **Item.Value 在 Redis 模式下不可重复**：底层使用 ZSET，相同 value 后推会覆盖前者。请通过额外字段（如自增 ID）保证唯一性。
- **VisibilityTimeout > Handler 最大耗时**：否则会重复派发。
- **死信不会自动清理 failed Hash**：`OnDeadLetter` 触发后 delayq 会执行 ack（清除 doing/failed/delay），无需手动处理。
- **时间轮粒度 1 秒**：内存队列最小延迟 1 秒；亚秒级延迟请考虑其他实现。

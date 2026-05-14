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

## 主要功能

| 功能 | API | 说明 |
|------|-----|------|
| 单条推送 | `Push(item)` | 普通延迟投递 |
| 批量推送 | `PushBatch(items)` | 原子批量投递（Redis 模式下走单 Lua） |
| 优先级 | `Item.Priority` | 同一执行时间点高优先级先派发 |
| 查询 | `Get(topic, value)` | 返回是否存在与剩余延迟 |
| 取消 | `Cancel(topic, value)` | 移除未派发的 item |
| 自动 ack | `Start(topic, handler)` | handler 返回 error 自动 ack/nack |
| 手动 ack | `StartManualAck(topic, handler)` | 业务显式调用 `Acker.Ack/Nack` |
| 重试退避 | `WithRetryBackoff` 等 | 固定/指数/自定义函数 |
| 死信 | `WithOnDeadLetter` | 重试耗尽回调 |
| 监控 | `WithMonitorCounter` + Prometheus Collector | 双通道指标 |

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

## 批量推送 / 查询 / 取消

```go
// 批量
dq.PushBatch([]*delayq.Item{
    {Topic: "orders", DelaySecond: 30, Value: []byte("o1")},
    {Topic: "orders", DelaySecond: 30, Value: []byte("o2")},
})

// 查询
remaining, exists, err := dq.Get("orders", []byte("o1"))
if exists {
    fmt.Printf("将在 %v 后执行\n", remaining)
}

// 取消（已开始执行的 handler 无法终止）
canceled, err := dq.Cancel("orders", []byte("o1"))
```

## 优先级

`Item.Priority` 在**同一执行时间点**生效，越大越先执行：

```go
dq.PushBatch([]*delayq.Item{
    {Topic: "t", DelaySecond: 1, Value: []byte("low"),  Priority: 1},
    {Topic: "t", DelaySecond: 1, Value: []byte("high"), Priority: 100},
    {Topic: "t", DelaySecond: 1, Value: []byte("mid"),  Priority: 50},
})
// 派发顺序：high → mid → low
```

> Priority 在 Redis 中通过 ZSET score 的微秒级偏移编码（`score = ts - priority * 1e-6`），不会跨秒错位。

## 手动 Ack/Nack

适合异步处理场景，handler 立即返回，由后台业务线程在合适时机 ack：

```go
dq.StartManualAck("orders", func(item *delayq.Item, ack delayq.Acker) {
    go func() {
        if err := processAsync(item); err != nil {
            ack.Nack(err)  // 触发重试或死信
            return
        }
        ack.Ack()         // 标记成功，从 doing 集移除
    }()
})
```

注意：
- 业务必须保证最终调用 `Ack` 或 `Nack`，否则该 item 会留在 doing 集直到 `VisibilityTimeout` 触发 reclaim 重新派发。
- 多次 Ack/Nack 是 no-op，安全。

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
    switch metric {
    case delayq.MetricHandleDurationMs:
        // 耗时上报：value 为毫秒数，可直接喂给 Histogram
        histogram.WithLabelValues(labels["Queue"]).Observe(float64(value) / 1000.0)
    default:
        counter.WithLabelValues(metric, labels["Queue"]).Add(float64(value))
    }
}))
```

可用 metric 名（也提供 `MetricXxx` 常量）：

| Metric | 类型 | 触发时机 |
|--------|------|---------|
| `delayq_produce` | Counter | Push 成功 |
| `delayq_produce_error` | Counter | Push 失败 |
| `delayq_handle` | Counter | Handler 处理成功 |
| `delayq_handle_error` | Counter | Handler 返回 error 或 panic |
| `delayq_handle_panic` | Counter | Handler 抛出 panic（已被恢复） |
| `delayq_handle_duration_ms` | Histogram observation | Handler 执行耗时（毫秒，单次 Observe） |
| `delayq_poll_error` | Counter | Redis poll 脚本失败 |
| `delayq_reclaim` | Counter | 一次 reclaim 搬运的 item 数 |
| `delayq_reclaim_error` | Counter | reclaim 脚本失败 |

> 注：`delayq_handle_panic` 与 `delayq_handle_error` 同时计数 panic 路径——前者用于精准报警 panic，后者用于通用失败率。

### Status() 实时状态

`Queue.Status()` 返回每个 topic 的等待数与在途处理数，便于自定义看板：

```go
s := dq.Status()
for topic, waiting := range s.QueueLength {
    inFlight := s.InFlight[topic]
    fmt.Printf("topic=%s waiting=%d in_flight=%d\n", topic, waiting, inFlight)
}
```

`Collector` 也会同时暴露两类 Gauge：`<Name>_status_queue_length` 与 `<Name>_status_in_flight`。

## 日志

通过 `WithLogger` 注入自定义实现：

```go
dq := delayq.New(delayq.WithLogger(myLogger))     // 接入 zap/logrus/slog
dq := delayq.New(delayq.WithLogger(delayq.NopLogger())) // 关闭日志
```

默认输出到 stderr，前缀 `[delayq]`。

## 预设（Preset）

针对常见场景提供一组开箱即用的 Option 组合：

```go
// 高吞吐：>=10k QPS Push（订单、通知、日志缓冲）
dq := delayq.New(delayq.HighThroughputPreset()...)

// 低延迟：实时派发（IM、推送）
dq := delayq.New(delayq.LowLatencyPreset()...)

// 高可靠：消息不能丢（金融、对账）
dq := delayq.New(append(delayq.ReliablePreset(),
    delayq.WithOnDeadLetter(persistDeadLetter), // 必须设置死信回调
)...)

// 与额外 Option 组合（后者覆盖前者）
dq := delayq.New(append(delayq.HighThroughputPreset(),
    delayq.WithName("myapp"),
    delayq.WithMaxConcurrency(2048), // 覆盖 preset 中的默认值
)...)
```

各预设详情：

| Preset | DisableValueIndex | MaxConcurrency | RetryTimes | VisibilityTimeout | 说明 |
|--------|-------------------|----------------|------------|-------------------|------|
| `HighThroughputPreset` | ✅ | 1024 | 3 | 5min | Get/Cancel 不可用，Push 提速 ~40% |
| `LowLatencyPreset` | ✗ | 256 | 2 | 30s | 短重试间隔，崩溃快速恢复 |
| `ReliablePreset` | ✗ | 64 | 15 | 15min | 长重试 + 大容错窗口 |

## 限流

```go
dq := delayq.New(
    delayq.WithPushRatePerSec(1000), // 每秒最多 1000 次 Push
    delayq.WithPushBurst(2000),      // 突发桶容量 2000
)

err := dq.Push(item)
if errors.Is(err, delayq.ErrRateLimited) {
    // 当前过载，业务自行选择 retry/drop/log
}
```

`PushBatch` 一次扣 N 个 token，不足时整批拒绝（不会部分入队）。

## 优雅退出

```go
// 触发关闭信号后：
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 方式一：仅等待消化（不关闭，Drain 后还能继续 Push）
err := dq.Drain(ctx)

// 方式二：等待消化后关闭（推荐）
err := dq.CloseGracefully(ctx)
```

Drain 期间：
- 新 `Push` 返回 `ErrDraining`
- 现有 item 继续被 ticker 派发与 handler 处理
- 等到 Length=0 且 InFlight=0 才返回
- ctx 超时返回 `ctx.Err()`，剩余 item 仍在队列中

## 完整示例

参见 [`examples/`](examples/) 目录：

- [`memory/`](examples/memory) - 最小内存队列
- [`redis/`](examples/redis) - 分布式 Redis 队列（基于 redisson）
- [`manualack/`](examples/manualack) - 手动 ack/nack 异步处理
- [`graceful/`](examples/graceful) - 优雅退出 + Prometheus

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
| `WithDisableValueIndex(bool)` | `false` | 禁用 byValue 索引（Get/Cancel 不可用），Push 性能 +40% |
| `WithPushRatePerSec(float64)` | `0` | Push 限流速率（token/s），`<=0` 不限流 |
| `WithPushBurst(float64)` | `0` | Push 限流桶容量，`<=0` 取 PushRatePerSec |

## 性能

### 内存队列

| 操作 | macOS M2 Pro | Linux x86_64 (CI) | allocs |
|------|------:|------:|------:|
| Push（默认，含 byValue 索引） | 490 ns/op (2.0M ops/s) | 见 [Actions Artifact][bench] | 6 |
| Push（`WithDisableValueIndex`） | **218 ns/op** (4.6M ops/s) | 见 [Actions Artifact][bench] | 3 |
| PushBatch (1000 items) | 533 μs (**1.9M items/s**) | 见 [Actions Artifact][bench] | 6/item |
| Get | 69 ns (14M ops/s) | 见 [Actions Artifact][bench] | 1 |
| Cancel | 248 ns (4.0M ops/s) | 见 [Actions Artifact][bench] | 1 |
| Length | **15 ns** (67M ops/s) | 见 [Actions Artifact][bench] | 0 |
| 时间轮 sweep (1000 due) | 549 μs (1.8M items/s) | 见 [Actions Artifact][bench] | 3.9/item |

### Redis 队列

Push 性能受 redisson 客户端 + 网络 RTT 主导。本地 docker redis 6.2 测得：

| 操作 | macOS M2 Pro + 本地 Docker | Linux + service container | 备注 |
|------|------:|------:|------|
| Push | 156 μs (~6.4k ops/s) | 见 [Actions Artifact][bench] | 单连接 |
| PushBatch (1000) | 见 Actions | 见 [Actions Artifact][bench] | 单 Lua 脚本 |
| Length | 166 μs | 见 [Actions Artifact][bench] | 一次 ZCARD x2 |

[bench]: https://github.com/sandwich-go/delayq/actions/workflows/benchmark.yml

### 本地跑 benchmark

```bash
# 内存
go test -bench=. -benchmem -benchtime=1s ./...

# Redis（需要本地 Redis）
make test-integration                                 # 仅集成测试
REDIS_ADDR=127.0.0.1:6379 go test -tags=integration \
  -run=^$ -bench=BenchmarkRedisQueue -benchmem .      # Redis benchmark
```

`WithDisableValueIndex` 在不需要 `Get`/`Cancel` 的高吞吐场景下推荐启用，可省一次 map 写入与一次 string 拷贝（Push 提速 ~55%）。

## 注意事项

### Topic 路由

`Queue.Push(item)` 的路由规则：

1. `Item.Topic` 非空 → 路由到该 topic（不存在则返回 `ErrTopicQueueHasClosed`）
2. `Item.Topic` 为空 + 仅注册一个 topic → 自动路由并回填 `Item.Topic`
3. `Item.Topic` 为空 + 多个 topic / 0 个 topic → 拒绝（`ErrTopicQueueHasClosed`）

直接使用 `TopicQueue.Push` 时，`Item.Topic` 会被强制覆盖为该 TopicQueue 的 topic 名（用户错填的会有 DEBUG 日志）。

### Value 大小

- 强烈建议 `Item.Value <= 1KB`，仅存储业务 ID/引用，不要把完整 payload 写入。
- 单个 Value `>= 4KB` 会触发 WARN 日志（不阻断）。
- 在 Redis 模式下 ZSET 成员太大会显著拖慢 ZRANGEBYSCORE。

### 其他

- **Item.Value 在 Redis 模式下不可重复**：底层使用 ZSET，相同 value 后推会覆盖前者。请通过额外字段（如自增 ID）保证唯一性。
- **VisibilityTimeout > Handler 最大耗时**：否则会重复派发。
- **死信不会自动清理 failed Hash**：`OnDeadLetter` 触发后 delayq 会执行 ack（清除 doing/failed/delay），无需手动处理。
- **时间轮粒度 1 秒**：内存队列最小延迟 1 秒；亚秒级延迟请考虑其他实现。
- **OnDeadLetter 回调 panic 会被捕获**：用户回调 panic 不会导致队列崩溃，会以 ERROR 日志记录。
- **ticker 错误指数退避**：Redis poll/reclaim 或时间轮 tick 出错时会自动退避，最长 30 秒，恢复后立即重置。

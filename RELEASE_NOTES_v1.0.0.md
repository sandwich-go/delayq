# delayq v1.0.0 Release Notes

**首个生产可用版本**。从原型级 4.5/10 演进至生产可用 8.5+/10，经过 8 轮系统性改造。

## 亮点

- **51× Push 性能提升**（30000 ns/op → 548 ns/op；no-index 模式 224 ns/op）
- **103+ 测试用例 + 12 个 benchmark + 4 个 fuzz**，覆盖率 91%+，`-race -count=2` 稳定
- **完整可观测性**：Prometheus Collector + 9 个 metric 常量 + Status() 实时状态
- **完整生命周期**：`Drain` 优雅退出 + `CloseGracefully` + 限流 + Cancel
- **零 lint warning**：vet / staticcheck / errcheck / golangci-lint v2 全部 clean
- **GitHub Actions CI**：3 个 Go 版本矩阵 (1.20/1.21/1.22) + race + coverage

## 主要功能

| 功能 | API |
|------|-----|
| 内存延迟队列 | `delayq.New()` 时间轮粒度 1s |
| 分布式延迟队列 | `WithRedisScriptBuilder(builder)` 接入任意 Redis 客户端 |
| 单条 / 批量推送 | `Push(item)` / `PushBatch(items)` |
| 优先级 | `Item.Priority`（同执行时间内排序） |
| 查询 / 取消 | `Get(topic, value)` / `Cancel(topic, value)` |
| 自动 Ack | `Start(topic, handler)` handler 返回 error 自动重试 |
| 手动 Ack | `StartManualAck(topic, handler)` 异步处理 |
| 重试退避 | 固定间隔 / 指数退避 / 自定义函数 |
| 死信回调 | `WithOnDeadLetter(func)` |
| 限流 | `WithPushRatePerSec(rate, burst)` token bucket |
| 优雅退出 | `Drain(ctx)` / `CloseGracefully(ctx)` |
| 监控 | Prometheus Collector + MonitorCounter（9 个 metric） |
| 日志 | `WithLogger(impl)` 注入 zap/logrus/slog |

## 性能（Apple M2 Pro / Go 1.25）

| 操作 | ns/op | ops/s | allocs/op |
|------|------|-------|-----------|
| Push（默认） | ~550 | **1.8M/s** | 6 |
| Push（DisableValueIndex） | ~225 | **4.4M/s** | 3 |
| PushBatch (1000) | ~488 μs | **2M items/s** | 6/item |
| Get / Cancel | ~70 / ~234 | 14M / 4M | 1 |
| Length | ~15 | 67M | 0 |

## 主要架构改进（按版本）

| Commit | 改进 |
|--------|------|
| `50ccea0` | 修复内存/Redis 队列并发与资源管理 5 个致命 bug |
| `d14013d` | 测试覆盖率 66.7% → 90.8%，新增 8 个测试文件 |
| `a9fde51` | 暴露 `VisibilityTimeout` / 退避重试 |
| `c04c961` | 工程治理：CI / golangci-lint / Makefile / proto 源 / 依赖收敛 |
| `736458a` | 健壮性 (B7-B11)：Topic 路由 / 并发边界 / ticker 退避 / panic 保护 / Value 警示 |
| `d7adeb3` | 功能扩展 (C12-C16)：PushBatch / Acker / Priority / Get / Cancel |
| `738717a` | 可观测性 (D17-D19)：handle_panic / handle_duration_ms / in_flight |
| `56b1819` | 性能 (F1-F3)：51× Push 提升 |
| (本版本) | F4-F8：限流 / Drain / examples / fuzz + chaos |

## BREAKING CHANGES

### 与原 0.x 版本数据格式不兼容

**Redis 数据格式变化**（0.1.x → 1.0.0）：
- 失败计数从 ZSET 负 score 编码改为独立 Hash `<prefix>:failed:{<topic>}`
- ZSET score 类型从 int64 改为 float64（priority 编码需要）
- ZSET score 现在严格表示"预期执行时间戳"，不再有负值语义

**升级建议**：
```bash
# 升级前清空旧数据（生产环境请谨慎）
redis-cli DEL "__dq:do:{<topic>}" "__dq:doing:{<topic>}"
```

### 公开 API 扩展

`TopicQueue` 与 `Queue` 接口新增方法，自定义实现需要补全：
- `PushBatch / Get / Cancel / InFlight / StartManualAck / Drain`

如果你只通过 `delayq.New()` 使用本库，无需任何代码修改。

## 升级指南

```go
// 0.x
dq := delayq.New(delayq.WithRedisScriptBuilder(builder))
dq.Start("topic", handler)
dq.Push(&delayq.Item{Topic: "topic", DelaySecond: 30, Value: payload})
dq.Close()

// 1.0
dq := delayq.New(
    delayq.WithRedisScriptBuilder(builder),
    delayq.WithVisibilityTimeout(2*time.Minute),     // 新：之前硬编码 10min
    delayq.WithMaxConcurrency(64),                   // 新：单 topic 并发上限
    delayq.WithRetryBackoff(2.0),                    // 新：指数退避
)
dq.Start("topic", handler)
dq.Push(&delayq.Item{Topic: "topic", DelaySecond: 30, Value: payload})

// 优雅退出
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
dq.CloseGracefully(ctx)
```

## 已知限制

- **内存队列时间轮粒度 1 秒**，亚秒级延迟请考虑其他实现
- **Redis 模式 Item.Value 不可重复**（ZSET 成员唯一），相同 value 后推会覆盖前者
- **VisibilityTimeout 必须 > Handler 最大耗时**，否则会重复派发

## 完整变更日志

见 [CHANGELOG.md](CHANGELOG.md)。

## 升级路径

```bash
go get github.com/sandwich-go/delayq@v1.0.0
```

## 致谢

历经 8 轮系统性改造完成本版本，详见 git log。

# Changelog

本文档遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [1.0.0] - 2026-05-14

首个生产可用版本，详见 [RELEASE_NOTES_v1.0.0.md](RELEASE_NOTES_v1.0.0.md)。

### Added (心跳自动延期)

- **Heartbeat 自动延期**：Redis 模式下 handler 执行期间自动每 `VisibilityTimeout/3`（至少 1s）通过 `ZADD XX` 刷新 doing 集 score，**长任务不再需要把 VisibilityTimeout 调极大也不会被重复派发**。
  - 新增 `WithHeartbeatInterval(d)` Option：默认 `0` = `VisibilityTimeout/3`；`-1` 显式禁用
  - 业务 Ack 后心跳自动退出（通过 ZSCORE 检测），不会复活已删除 item
  - 进程崩溃 → 心跳停止 → reclaim 接手 → 重新派发，恢复语义清晰
  - 手动 ack 模式下心跳仅覆盖 handler 函数返回前的同步阶段
  - 新增 metric：`MetricHeartbeat` / `MetricHeartbeatError`
  - 新增 `baseQueue.onItemStart` 钩子（私有），允许后端注入 per-item 生命周期扩展

### Added (本轮新增 - F4/F5/F6/F8)

- **F4 限流**：`WithPushRatePerSec(rate)` + `WithPushBurst(burst)` token bucket，超限返回 `ErrRateLimited`。新增 `MetricRateLimited` 指标。
- **F5 优雅退出**：
  - `TopicQueue.Drain(ctx)` / `Queue.Drain(ctx)` 拒绝新 Push 并等待消化
  - `Queue.CloseGracefully(ctx)` = Drain + Close
  - 新增 `ErrDraining` 错误
- **F6 examples/**：4 个独立可运行示例（memory / redis / manualack / graceful）
- **F8 fuzz + chaos**：`FuzzPush / FuzzPushBatch / FuzzParseInt64 / FuzzComputeRetryDelay` + 4 个 chaos 测试

### Fixed (本轮)

- 通过 fuzz 发现并修复 `computeRetryDelay` 在 `MaxRetryInterval<0` 时返回负值的 bug
- 优化 `inFlight` 计数时机：从子 goroutine 移到 `execute` 派发线程，避免 Drain 早退

### Performance (性能)

- **Push 路径吞吐 51× 提升**：
  - 同 priority 下链表头插改为 O(1)（原 O(N)）：`head.priority <= n.priority` 时直接前插
  - 移除 wheelNode.id 字符串字段及 idSeq 计数器，少 1 次 strconv 分配
  - `query map[string]struct{}` 改为 `count int64` 计数器，省一次 map 写
  - benchmark：30000 ns/op → 548 ns/op（with-index），224 ns/op（no-index）
- **`WithDisableValueIndex(bool)` Option**：禁用 byValue 索引（Get/Cancel 不可用），Push 进一步提速 ~40%。
- **完整 benchmark 套件**：新增 `benchmark_test.go` 覆盖 Push/PushBatch/Get/Cancel/Length/Status/TickerSweep/HandlerThroughput/ManualAck/Priority 等 12 个场景。

### Added (新增)

- **PushBatch**：批量推送 API，Redis 模式下通过单次 Lua 脚本原子写入。
- **优先级**：`Item.Priority`（int32），同一执行时间点高优先级先派发。Redis 模式下使用 `score = exec_ts - priority * 1e-6` 编码，不会跨秒错位。
- **Get(topic, value)**：查询某 value 是否存在于队列中，返回剩余延迟与存在标记。
- **Cancel(topic, value)**：取消队列中匹配 value 的 item（覆盖 delay 与 doing 集，已开始的 handler 无法终止）。
- **手动 ack 模式**：`StartManualAck(topic, func(item, Acker))`，业务显式调用 `Acker.Ack/Nack`，适合异步处理。
- **新监控指标**：
  - `delayq_handle_panic` (Counter)：精准计数 handler panic 次数
  - `delayq_handle_duration_ms` (Histogram observation)：handler 执行耗时毫秒数
  - `delayq_status_in_flight` (Gauge)：每 topic 当前在途 handler 数
- **`Status.InFlight`**：`Status()` 返回结构新增 `InFlight map[string]int64` 字段。
- **`MetricXxx` 常量**：提供所有 metric 名的 `MetricProduce`/`MetricHandlePanic` 等常量，避免拼写错误。
- **CI/CD**：GitHub Actions 流水线（lint/test/coverage 三 job），golangci-lint v2 配置 + 11 个 linter，Makefile 标准化构建命令。
- **proto 源**：补充 `item.proto` 源文件 + `make proto` 生成命令。
- **`.gitignore`**：覆盖 IDE/coverage/OS 常见忽略项。

### Changed (改动)

- **Item.proto** 新增 `priority` 字段（field 4），向后兼容（旧客户端忽略未知字段）。
- **Redis ZSET score 改为 float64**：原 int64 score 在加入 priority 后改为 float64（`exec_ts - priority * 1e-6`），不影响秒级排序。
- **Queue 接口扩展**：新增 `PushBatch / Get / Cancel / StartManualAck` 四个方法。
- **TopicQueue 接口扩展**：新增 `PushBatch / Get / Cancel / InFlight / StartManualAck` 五个方法。
- **依赖**：`go.mod` 锁定 `redisson v1.2.22`（兼容 go 1.20），生产代码不直接 import。
- **panic 计数**：handler panic 现在同时计入 `handle_error` 和 `handle_panic`，便于报警同时拿到 panic 与总错误率。

### Fixed (修复)

- **commit message 编码**：修正历史 commit 中文乱码（force push 重写）。

## [0.1.x] - 健壮性增强（已合并）

### Added

- **VisibilityTimeout**：`WithVisibilityTimeout(d)` 暴露 Redis 处理超时（替代硬编码 `safeSec=10min`）。
- **退避重试**：`WithRetryInterval / WithRetryBackoff / WithMaxRetryInterval / WithRetryIntervalFunc`。
- **MaxConcurrency**：`WithMaxConcurrency(n)` 限制单 topic 业务 handler 并发（默认 256）。
- **Logger 接口**：`WithLogger(impl)` 注入 zap/logrus/slog 等；提供 `NopLogger()`。
- **OnDeadLetter panic 保护**：用户回调 panic 不会导致 goroutine 崩溃。
- **Topic 路由**：`Item.Topic` 为空且仅注册一个 topic 时自动路由并回填。
- **大 Value 警示**：Push 时 `len(Value) >= 4KB` 触发 WARN 日志（不阻断）。
- **ticker 错误退避**：连续失败时指数退避，上限 30s，恢复后立即重置。
- **Redis 失败计数 Hash**：从负 score 编码改为独立 `failed:{topic}` Hash，语义清晰。

### Changed (BREAKING)

- **Redis 数据格式**：失败计数从 ZSET 负 score 改为 Hash；旧版本数据需清理后才能升级。
- **`go.mod`**：从 `go 1.19` 升级到 `go 1.20`，`redisson` 提升为主依赖（仅测试使用）。

### Fixed

- **内存队列链表删除 bug**：连续到期节点删除导致链表断裂。
- **`Length()` bug**：`wheelNode.Id` 从未赋值，导致 `query` map 永远只存一个键。
- **数据竞争**：ticker 与 Push 并发访问 wheels/query。
- **double close panic**：ctx cancel 与显式 Close 同时触发 close(exitC)。
- **handler goroutine leak**：execute 在 Close 期间继续 spawn。
- **Redis context 不传递**：所有 Redis 调用从 `context.Background()` 切换到 `q.ctx`。

## 0.1.0 - 初始版本

- 基础内存延迟队列（时间轮，1s 粒度）
- 基础 Redis 延迟队列（基于 ZSET + Lua）
- Prometheus Collector

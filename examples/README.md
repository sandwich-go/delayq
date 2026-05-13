# delayq examples

每个示例都是独立的 `main` 程序，可直接 `go run`：

| 目录 | 说明 |
|------|------|
| `memory/` | 最小内存队列示例：Push + 消费 + 优先级 + Get/Cancel |
| `redis/` | 分布式 Redis 队列示例（基于 redisson 适配器） |
| `manualack/` | 手动 ack/nack 异步处理示例 |
| `graceful/` | 优雅退出（Drain + Close）示例，含 Prometheus Collector |

运行单个示例：

```bash
cd examples/memory
go run .
```

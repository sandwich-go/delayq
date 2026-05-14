# delayq loadtest

长跑稳定性压测工具，用于验证 delayq 在生产规模负载下的稳定性、吞吐量与延迟。

## 用法

### 快速验证（5 分钟内存模式）

```bash
go run ./cmd/loadtest -duration=5m -qps=2000
```

### 24 小时长跑

```bash
go run ./cmd/loadtest -duration=24h -qps=5000 -interval=1m -snapshot=loadtest-24h.json
```

### Redis 模式

```bash
# 启动 Redis（如已启动跳过）
docker run -d --rm -p 6379:6379 --name lt-redis redis:7-alpine

# 运行
go run ./cmd/loadtest \
    -redis=127.0.0.1:6379 \
    -duration=4h \
    -qps=2000 \
    -error-rate=0.01 \
    -panic-rate=0.001 \
    -snapshot=loadtest-redis.json
```

### 高吞吐场景（带限流验证）

```bash
go run ./cmd/loadtest \
    -preset=highthroughput \
    -duration=30m \
    -qps=20000 \
    -producers=16
```

## 完整参数

```
-mode             memory | redis（默认 memory；指定 -redis 时自动切换）
-redis            Redis 地址（如 127.0.0.1:6379）
-preset           default | highthroughput | lowlatency | reliable
-topic            topic 名（默认 loadtest）
-duration         运行时长（如 1h / 24h；0 = 永远）
-qps              全部 producer 合计目标 QPS（默认 1000）
-producers        生产者 goroutine 数（默认 4）
-max-delay        item DelaySecond 上限（默认 5s）
-error-rate       handler 注入 error 比例 [0,1]（默认 0）
-panic-rate       handler 注入 panic 比例 [0,1]（默认 0）
-handle-time      handler 模拟处理耗时（默认 1ms）
-interval         状态打印间隔（默认 10s）
-snapshot         JSON 快照输出路径（默认 loadtest-snapshot.json）
-prom-addr        Prometheus metrics 监听地址（如 :9100；空=禁用）
-value-size       Item.Value 字节数（默认 64）
```

## 实时输出

```
ELAPSED  PUSH_OK  PUSH_QPS  PUSH_p99  HANDLE_OK  HANDLE_QPS  HANDLE_p99  E2E_p50  E2E_p99  Q_LEN  IN_FLIGHT  DLQ  ERR  PANIC  RATE_LIM  GOROUT  HEAP_MB
   10s    19896      1983    43.7µs       13021        1953     6.28ms   562ms   1.05s   6877          0    0    0      0         0       9        6
   20s    39844      1996    41.9µs       33250        2020     4.01ms   560ms   1.07s   6594          0    0    0      0         0       9        5
```

各列含义：
- `ELAPSED`：累计运行秒数
- `PUSH_OK / HANDLE_OK`：累计成功 push / handle 数
- `PUSH_QPS / HANDLE_QPS`：本次 interval 平均 QPS
- `*_p99`：本次 interval 内的 p99 延迟（push 自身耗时 / handler 处理耗时）
- `E2E_p50/p99`：从 push 时刻 + delay 到 handler 实际执行的端到端延迟
- `Q_LEN / IN_FLIGHT`：当前 delay 集长度 / 在途 handler 数
- `DLQ / ERR / PANIC / RATE_LIM`：累计死信 / 错误 / panic / 限流计数
- `GOROUT / HEAP_MB`：goroutine 数 / heap 字节（MB）

## 信号

| 信号 | 行为 |
|------|------|
| `SIGINT` / `SIGTERM` (Ctrl-C) | 优雅退出：停止生产者 → drain → 写最终 snapshot |
| `SIGUSR1` | 中途 dump 当前快照到 `-snapshot` 文件（持续运行） |

## 健康检查

退出时会自动评估健康状态，不通过则进程返回非 0：

- `queue_length` 与 `in_flight` 在 drain 后都应为 0
- 派发数应接近推送数（允许 1% 缓冲 + 10）
- `pending_track` 跟踪残留不应过多（按 push QPS × 5s 估算）
- goroutine 不应超过 10000（粗粒度泄漏检测）

## Snapshot JSON

```json
{
  "time": "2026-05-14T10:20:54+08:00",
  "elapsed_sec": 36.0,
  "push_ok": 59808,
  "push_error": 0,
  "handle_ok": 59808,
  "handle_error": 0,
  "handle_panic": 0,
  "dead_letter": 0,
  "rate_limited": 0,
  "push_qps": 1685.0,
  "handle_qps": 1685.0,
  "push_latency": {"count": 8192, "p50": 3541, "p99": 41875, "p999": 1572917, "max": 5246209},
  "handle_latency": {...},
  "e2e_latency": {...},
  "queue_length": 0,
  "in_flight": 0,
  "pending_track": 0,
  "goroutines": 4,
  "heap_bytes": 2097152,
  "total_alloc_bytes": 107374182,
  "num_gc": 31,
  "pause_total_ns": 8300000,
  "metrics": {"delayq_handle": 59808, "delayq_produce": 59808}
}
```

## 与 Prometheus 集成

```bash
# 启动时暴露 metrics
go run ./cmd/loadtest -prom-addr=:9100 -duration=24h

# 在 prometheus.yml 加 scrape config:
# - job_name: delayq-loadtest
#   static_configs:
#     - targets: ['localhost:9100']
```

会暴露 delayq Collector（`delayq_status_queue_length` + `delayq_status_in_flight`）。

## Makefile 快捷方式

```bash
make loadtest          # 5 分钟内存模式
make loadtest-24h      # 24 小时内存模式（5k QPS）
make loadtest-redis    # 10 分钟 Redis 模式
```

## 验收标准（24h 长跑）

| 指标 | 期望 |
|------|------|
| Push 错误率 | 0%（除非主动注入限流） |
| Handle 错误率 | ≈ `error_rate` 注入值 |
| 残留 item（drain 后） | 0 |
| Goroutine 数（最终） | < 50（生产者退出后） |
| Heap 增长（前后对比） | < 50% （无内存泄漏） |
| GC 暂停（per cycle） | < 10ms |
| E2E p99 延迟 | < `max-delay` + 1s |

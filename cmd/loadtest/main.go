// Command loadtest 是 delayq 的长跑稳定性压测工具。
//
// 用法：
//
//	# 内存队列，10 producer + 默认消费，跑 1 小时
//	go run ./cmd/loadtest -duration=1h
//
//	# Redis 队列（需要本地 Redis）
//	go run ./cmd/loadtest -redis=127.0.0.1:6379 -duration=24h -qps=5000
//
//	# 高吞吐压测，5% handler 失败率
//	go run ./cmd/loadtest -preset=highthroughput -error-rate=0.05 -qps=20000
//
// 退出方式：
//   - SIGINT / SIGTERM：触发 graceful drain，等待消化
//   - 达到 -duration：自动 graceful drain
//
// 输出：
//   - 每 -interval 打印一行 TSV 表格到 stdout
//   - SIGUSR1：dump 当前快照到 -snapshot 文件（JSON）
//   - 退出时打印 final summary，并写入 final snapshot
//   - 可选 Prometheus endpoint（-prom-addr）
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sandwich-go/delayq"
)

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("loadtest failed: %v", err)
	}
}

type config struct {
	mode        string // "memory" or "redis"
	redisAddr   string
	preset      string // "default" / "highthroughput" / "lowlatency" / "reliable"
	topic       string
	duration    time.Duration
	qps         int
	producers   int
	maxDelay    time.Duration
	errorRate   float64
	panicRate   float64
	handleTime  time.Duration
	interval    time.Duration
	snapshot    string
	promAddr    string
	maxValueLen int
}

func parseFlags() *config {
	cfg := &config{}
	flag.StringVar(&cfg.mode, "mode", "memory", "queue backend: memory | redis")
	flag.StringVar(&cfg.redisAddr, "redis", "", "redis address (sets mode=redis)")
	flag.StringVar(&cfg.preset, "preset", "default", "option preset: default|highthroughput|lowlatency|reliable")
	flag.StringVar(&cfg.topic, "topic", "loadtest", "topic name")
	flag.DurationVar(&cfg.duration, "duration", 5*time.Minute, "total run duration; 0 means forever")
	flag.IntVar(&cfg.qps, "qps", 1000, "target push rate per second across all producers")
	flag.IntVar(&cfg.producers, "producers", 4, "number of producer goroutines")
	flag.DurationVar(&cfg.maxDelay, "max-delay", 5*time.Second, "max DelaySecond per item (random within [1, max])")
	flag.Float64Var(&cfg.errorRate, "error-rate", 0, "fraction of handler invocations that return error [0,1]")
	flag.Float64Var(&cfg.panicRate, "panic-rate", 0, "fraction of handler invocations that panic [0,1]")
	flag.DurationVar(&cfg.handleTime, "handle-time", 1*time.Millisecond, "simulated handler processing time")
	flag.DurationVar(&cfg.interval, "interval", 10*time.Second, "stats print interval")
	flag.StringVar(&cfg.snapshot, "snapshot", "loadtest-snapshot.json", "JSON snapshot output file (final + SIGUSR1)")
	flag.StringVar(&cfg.promAddr, "prom-addr", "", "Prometheus metrics listen address (empty=disabled, e.g. :9100)")
	flag.IntVar(&cfg.maxValueLen, "value-size", 64, "item Value length in bytes")
	flag.Parse()

	if cfg.redisAddr != "" {
		cfg.mode = "redis"
	}
	if cfg.qps < 1 {
		cfg.qps = 1
	}
	if cfg.producers < 1 {
		cfg.producers = 1
	}
	return cfg
}

func run(cfg *config) error {
	log.Printf("loadtest config: %+v", cfg)
	log.Printf("Go: %s, GOMAXPROCS=%d, NumCPU=%d", runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU())

	stats := newStats()

	// 构造 delayq 选项（preset + 监控注入）
	opts := buildOptions(cfg, stats)

	// Redis 后端
	if cfg.mode == "redis" {
		if cfg.redisAddr == "" {
			return errors.New("-redis is required when mode=redis")
		}
		opts = append(opts, delayq.WithRedisScriptBuilder(newRedisBuilder(cfg.redisAddr)))
	}

	dq := delayq.New(opts...)
	defer func() {
		_ = dq.Close()
	}()

	// 启动 handler
	if err := dq.Start(cfg.topic, makeHandler(cfg, stats)); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	// 可选 Prometheus endpoint
	if cfg.promAddr != "" {
		prometheus.MustRegister(dq.Collector())
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Printf("prom: serving %s/metrics", cfg.promAddr)
			if err := http.ListenAndServe(cfg.promAddr, nil); err != nil {
				log.Printf("prom http: %v", err)
			}
		}()
	}

	// 启动生产者
	ctx, cancelProducers := context.WithCancel(context.Background())
	producerDone := startProducers(ctx, cfg, dq, stats)

	// 启动 stats printer
	statsCtx, cancelStats := context.WithCancel(context.Background())
	go statsPrinter(statsCtx, cfg, stats, dq)

	// SIGUSR1 → dump snapshot
	usrCh := make(chan os.Signal, 1)
	signal.Notify(usrCh, syscall.SIGUSR1)
	go func() {
		for range usrCh {
			if err := writeSnapshot(cfg.snapshot, stats.snapshot(dq)); err != nil {
				log.Printf("snapshot: %v", err)
			} else {
				log.Printf("snapshot written to %s", cfg.snapshot)
			}
		}
	}()

	// 退出信号
	exitCh := make(chan os.Signal, 1)
	signal.Notify(exitCh, syscall.SIGINT, syscall.SIGTERM)

	var deadlineCh <-chan time.Time
	if cfg.duration > 0 {
		deadlineCh = time.After(cfg.duration)
	}

	select {
	case sig := <-exitCh:
		log.Printf("received %v, draining...", sig)
	case <-deadlineCh:
		log.Printf("duration %v reached, draining...", cfg.duration)
	}

	// 停止生产者，开始 drain
	cancelProducers()
	producerDone()

	// 给消费一些时间消化（5min 上限，避免无限等）
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer drainCancel()

	startDrain := time.Now()
	if err := dq.Drain(drainCtx); err != nil {
		log.Printf("drain returned: %v (elapsed=%v)", err, time.Since(startDrain))
	} else {
		log.Printf("drained in %v", time.Since(startDrain))
	}

	cancelStats()

	// 健康评估 + final snapshot
	final := stats.snapshot(dq)
	printFinalSummary(final)
	if err := writeSnapshot(cfg.snapshot, final); err != nil {
		log.Printf("final snapshot: %v", err)
	} else {
		log.Printf("final snapshot written to %s", cfg.snapshot)
	}

	if h := assessHealth(final); !h.ok {
		log.Printf("HEALTH CHECK FAILED:")
		for _, r := range h.reasons {
			log.Printf("  - %s", r)
		}
		return errors.New("health check failed")
	}
	log.Printf("HEALTH CHECK PASSED")
	return nil
}

func buildOptions(cfg *config, stats *stats) []delayq.Option {
	var preset []delayq.Option
	switch cfg.preset {
	case "highthroughput":
		preset = delayq.HighThroughputPreset()
	case "lowlatency":
		preset = delayq.LowLatencyPreset()
	case "reliable":
		preset = delayq.ReliablePreset()
	case "default", "":
		preset = nil
	default:
		log.Fatalf("unknown preset: %s", cfg.preset)
	}
	opts := append([]delayq.Option(nil), preset...)
	opts = append(opts,
		delayq.WithLogger(delayq.NopLogger()),
		delayq.WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
			stats.observeMetric(metric, value)
		}),
		delayq.WithOnDeadLetter(func(item *delayq.Item) {
			stats.incDeadLetter()
		}),
	)
	return opts
}

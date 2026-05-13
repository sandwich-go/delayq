// 优雅退出示例：监听 SIGINT/SIGTERM，等待所有 in-flight 任务完成后再退出
// 同时演示 Prometheus Collector 集成（监听 :9090/metrics）
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandwich-go/delayq"
)

func main() {
	// 自定义 monitor 计数器（接入到 Prometheus Counter）
	pushedCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "myapp_delayq_pushed_total",
		Help: "Number of items pushed",
	}, []string{"queue", "result"})
	prometheus.MustRegister(pushedCounter)

	dq := delayq.New(
		delayq.WithName("myapp"),
		delayq.WithMaxConcurrency(32),
		delayq.WithMonitorCounter(func(metric string, value int64, labels prometheus.Labels) {
			switch metric {
			case delayq.MetricProduce:
				pushedCounter.With(prometheus.Labels{"queue": labels["Queue"], "result": "ok"}).Add(float64(value))
			case delayq.MetricProduceError:
				pushedCounter.With(prometheus.Labels{"queue": labels["Queue"], "result": "error"}).Add(float64(value))
			}
		}),
	)
	// 注册 Collector 暴露 queue_length / in_flight Gauge
	prometheus.MustRegister(dq.Collector())

	var processed int64
	if err := dq.Start("jobs", func(item *delayq.Item) error {
		// 模拟处理耗时
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&processed, 1)
		return nil
	}); err != nil {
		panic(err)
	}

	// 启动 metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":9090", nil)
	}()
	fmt.Println("Prometheus metrics: http://localhost:9090/metrics")

	// 持续推送任务
	stop := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = dq.Push(&delayq.Item{
				Topic:       "jobs",
				DelaySecond: 1,
				Value:       []byte(fmt.Sprintf("job-%d", i)),
			})
			i++
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// 等待退出信号
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nReceived signal, draining...")
	close(stop) // 停止生产者

	// 优雅退出：30s 内完成 drain
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dq.CloseGracefully(ctx); err != nil {
		fmt.Printf("graceful close failed: %v\n", err)
	}
	fmt.Printf("processed=%d\n", atomic.LoadInt64(&processed))
}

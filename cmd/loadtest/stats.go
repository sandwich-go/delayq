package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandwich-go/delayq"
)

// stats 跨 goroutine 共享的实时计数器与样本池。
//
// 计数器用 atomic int64；延迟样本用 reservoir-style 简化算法（环形缓冲）。
type stats struct {
	startedAt time.Time

	// counters
	pushOK      int64
	pushError   int64
	handleOK    int64
	handleError int64
	handlePanic int64
	deadLetter  int64
	rateLimited int64

	// 关联 metric（来自 dq.MonitorCounter）累计值
	metricSums sync.Map // map[string]*int64

	// 延迟样本环（push 自身耗时 / handler 处理耗时 / e2e 延迟）
	pushLat   *latencyRing
	handleLat *latencyRing
	e2eLat    *latencyRing

	// 待匹配的 expected exec time，用于 e2e 延迟统计
	pushTimes sync.Map // map[string]time.Time
}

func newStats() *stats {
	return &stats{
		startedAt: time.Now(),
		pushLat:   newLatencyRing(8192),
		handleLat: newLatencyRing(8192),
		e2eLat:    newLatencyRing(8192),
	}
}

func (s *stats) incPushOK()      { atomic.AddInt64(&s.pushOK, 1) }
func (s *stats) incPushError()   { atomic.AddInt64(&s.pushError, 1) }
func (s *stats) incHandleOK()    { atomic.AddInt64(&s.handleOK, 1) }
func (s *stats) incHandleError() { atomic.AddInt64(&s.handleError, 1) }
func (s *stats) incHandlePanic() { atomic.AddInt64(&s.handlePanic, 1) }
func (s *stats) incDeadLetter()  { atomic.AddInt64(&s.deadLetter, 1) }

func (s *stats) observePushLatency(d time.Duration)   { s.pushLat.observe(d) }
func (s *stats) observeHandleLatency(d time.Duration) { s.handleLat.observe(d) }
func (s *stats) observeE2ELatency(d time.Duration)    { s.e2eLat.observe(d) }

func (s *stats) observeMetric(metric string, value int64) {
	v, _ := s.metricSums.LoadOrStore(metric, new(int64))
	atomic.AddInt64(v.(*int64), value)
	if metric == delayq.MetricRateLimited {
		atomic.AddInt64(&s.rateLimited, value)
	}
}

func (s *stats) recordPushTime(key string, expected time.Time) {
	s.pushTimes.Store(key, expected)
}

func (s *stats) getPushTime(key string) (time.Time, bool) {
	v, ok := s.pushTimes.Load(key)
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

func (s *stats) deletePushTime(key string) {
	s.pushTimes.Delete(key)
}

// snapshot 收集当前所有指标的不可变副本
func (s *stats) snapshot(dq delayq.Queue) *snapshot {
	now := time.Now()
	elapsed := now.Sub(s.startedAt)
	pushOK := atomic.LoadInt64(&s.pushOK)
	pushErr := atomic.LoadInt64(&s.pushError)
	handleOK := atomic.LoadInt64(&s.handleOK)
	handleErr := atomic.LoadInt64(&s.handleError)
	handlePanic := atomic.LoadInt64(&s.handlePanic)
	deadLetter := atomic.LoadInt64(&s.deadLetter)
	rateLimited := atomic.LoadInt64(&s.rateLimited)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	st := dq.Status()
	var totalQ, totalIF int64
	for _, v := range st.QueueLength {
		totalQ += v
	}
	for _, v := range st.InFlight {
		totalIF += v
	}

	// 未匹配的 push（pushTimes 中残留），可能是 e2e 延迟太长还没派发，或丢失
	var pendingTrack int64
	s.pushTimes.Range(func(_, _ any) bool {
		pendingTrack++
		return true
	})

	metrics := map[string]int64{}
	s.metricSums.Range(func(k, v any) bool {
		metrics[k.(string)] = atomic.LoadInt64(v.(*int64))
		return true
	})

	return &snapshot{
		Time:            now,
		ElapsedSec:      elapsed.Seconds(),
		PushOK:          pushOK,
		PushError:       pushErr,
		HandleOK:        handleOK,
		HandleError:     handleErr,
		HandlePanic:     handlePanic,
		DeadLetter:      deadLetter,
		RateLimited:     rateLimited,
		PushQPS:         float64(pushOK) / elapsed.Seconds(),
		HandleQPS:       float64(handleOK) / elapsed.Seconds(),
		PushLatency:     s.pushLat.percentiles(),
		HandleLatency:   s.handleLat.percentiles(),
		E2ELatency:      s.e2eLat.percentiles(),
		QueueLength:     totalQ,
		InFlight:        totalIF,
		PendingTrack:    pendingTrack,
		Goroutines:      runtime.NumGoroutine(),
		AllocBytes:      int64(memStats.Alloc),
		HeapBytes:       int64(memStats.HeapAlloc),
		TotalAllocBytes: int64(memStats.TotalAlloc),
		NumGC:           int64(memStats.NumGC),
		PauseTotalNs:    int64(memStats.PauseTotalNs),
		Metrics:         metrics,
	}
}

// snapshot 单次采样
type snapshot struct {
	Time            time.Time          `json:"time"`
	ElapsedSec      float64            `json:"elapsed_sec"`
	PushOK          int64              `json:"push_ok"`
	PushError       int64              `json:"push_error"`
	HandleOK        int64              `json:"handle_ok"`
	HandleError     int64              `json:"handle_error"`
	HandlePanic     int64              `json:"handle_panic"`
	DeadLetter      int64              `json:"dead_letter"`
	RateLimited     int64              `json:"rate_limited"`
	PushQPS         float64            `json:"push_qps"`
	HandleQPS       float64            `json:"handle_qps"`
	PushLatency     latencyPercentiles `json:"push_latency"`
	HandleLatency   latencyPercentiles `json:"handle_latency"`
	E2ELatency      latencyPercentiles `json:"e2e_latency"`
	QueueLength     int64              `json:"queue_length"`
	InFlight        int64              `json:"in_flight"`
	PendingTrack    int64              `json:"pending_track"`
	Goroutines      int                `json:"goroutines"`
	AllocBytes      int64              `json:"alloc_bytes"`
	HeapBytes       int64              `json:"heap_bytes"`
	TotalAllocBytes int64              `json:"total_alloc_bytes"`
	NumGC           int64              `json:"num_gc"`
	PauseTotalNs    int64              `json:"pause_total_ns"`
	Metrics         map[string]int64   `json:"metrics"`
}

func writeSnapshot(path string, s *snapshot) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(s); encErr != nil {
		_ = f.Close()
		return encErr
	}
	return f.Close()
}

// statsPrinter 周期性打印 TSV 表头 + 行
func statsPrinter(ctx context.Context, cfg *config, s *stats, dq delayq.Queue) {
	t := time.NewTicker(cfg.interval)
	defer t.Stop()

	header := "ELAPSED\tPUSH_OK\tPUSH_QPS\tPUSH_p99\tHANDLE_OK\tHANDLE_QPS\tHANDLE_p99\tE2E_p50\tE2E_p99\tQ_LEN\tIN_FLIGHT\tDLQ\tERR\tPANIC\tRATE_LIM\tGOROUT\tHEAP_MB"
	fmt.Println(header)

	prev := s.snapshot(dq)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cur := s.snapshot(dq)
		printDelta(prev, cur, cfg.interval)
		prev = cur
	}
}

func printDelta(prev, cur *snapshot, interval time.Duration) {
	intervalSec := interval.Seconds()
	pushDelta := cur.PushOK - prev.PushOK
	handleDelta := cur.HandleOK - prev.HandleOK

	fmt.Printf("%6.0fs\t%d\t%.0f\t%v\t%d\t%.0f\t%v\t%v\t%v\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
		cur.ElapsedSec,
		cur.PushOK,
		float64(pushDelta)/intervalSec,
		cur.PushLatency.P99,
		cur.HandleOK,
		float64(handleDelta)/intervalSec,
		cur.HandleLatency.P99,
		cur.E2ELatency.P50,
		cur.E2ELatency.P99,
		cur.QueueLength,
		cur.InFlight,
		cur.DeadLetter,
		cur.HandleError,
		cur.HandlePanic,
		cur.RateLimited,
		cur.Goroutines,
		cur.HeapBytes/(1024*1024),
	)
}

func printFinalSummary(s *snapshot) {
	log.Printf("======= FINAL SUMMARY =======")
	log.Printf("elapsed:        %.0fs", s.ElapsedSec)
	log.Printf("push:           ok=%d err=%d (qps=%.0f)", s.PushOK, s.PushError, s.PushQPS)
	log.Printf("handle:         ok=%d err=%d panic=%d dead=%d (qps=%.0f)",
		s.HandleOK, s.HandleError, s.HandlePanic, s.DeadLetter, s.HandleQPS)
	log.Printf("rate_limited:   %d", s.RateLimited)
	log.Printf("push latency:   p50=%v p99=%v p999=%v max=%v", s.PushLatency.P50, s.PushLatency.P99, s.PushLatency.P999, s.PushLatency.Max)
	log.Printf("handle latency: p50=%v p99=%v p999=%v max=%v", s.HandleLatency.P50, s.HandleLatency.P99, s.HandleLatency.P999, s.HandleLatency.Max)
	log.Printf("e2e latency:    p50=%v p99=%v p999=%v max=%v", s.E2ELatency.P50, s.E2ELatency.P99, s.E2ELatency.P999, s.E2ELatency.Max)
	log.Printf("residual:       q_len=%d in_flight=%d pending_track=%d", s.QueueLength, s.InFlight, s.PendingTrack)
	log.Printf("runtime:        goroutines=%d heap=%dMB total_alloc=%dMB num_gc=%d gc_pause_ms=%.1f",
		s.Goroutines, s.HeapBytes/(1024*1024), s.TotalAllocBytes/(1024*1024), s.NumGC, float64(s.PauseTotalNs)/1e6)
	log.Printf("metrics: %v", s.Metrics)
	log.Printf("=============================")
}

// ===== latencyRing：轻量级延迟样本池 =====

type latencyRing struct {
	mu   sync.Mutex
	buf  []time.Duration
	head int
	full bool
	max  time.Duration
}

func newLatencyRing(size int) *latencyRing {
	return &latencyRing{buf: make([]time.Duration, size)}
}

func (r *latencyRing) observe(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.head] = d
	r.head++
	if r.head >= len(r.buf) {
		r.head = 0
		r.full = true
	}
	if d > r.max {
		r.max = d
	}
}

type latencyPercentiles struct {
	Count int           `json:"count"`
	P50   time.Duration `json:"p50"`
	P90   time.Duration `json:"p90"`
	P99   time.Duration `json:"p99"`
	P999  time.Duration `json:"p999"`
	Max   time.Duration `json:"max"`
}

func (r *latencyRing) percentiles() latencyPercentiles {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.head
	if r.full {
		n = len(r.buf)
	}
	if n == 0 {
		return latencyPercentiles{}
	}
	cp := make([]time.Duration, n)
	copy(cp, r.buf[:n])
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return latencyPercentiles{
		Count: n,
		P50:   cp[n*50/100],
		P90:   cp[min1(n*90/100, n-1)],
		P99:   cp[min1(n*99/100, n-1)],
		P999:  cp[min1(n*999/1000, n-1)],
		Max:   r.max,
	}
}

func min1(a, b int) int {
	if a < b {
		return a
	}
	return b
}

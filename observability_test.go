package delayq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ===== D17: handle_panic metric 区分 panic 与 error =====

func TestMetric_HandlePanic_Recorded(t *testing.T) {
	var panicCount, errCount int64
	q := New(
		WithRetryTimes(0),
		WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
			switch metric {
			case MetricHandlePanic:
				atomic.AddInt64(&panicCount, value)
			case MetricHandleError:
				atomic.AddInt64(&errCount, value)
			}
		}),
	)
	defer q.Close()

	doneCh := make(chan struct{}, 2)
	if err := q.Start("p", func(item *Item) error {
		switch string(item.GetValue()) {
		case "panic":
			defer func() { doneCh <- struct{}{} }()
			panic("kaboom")
		case "err":
			defer func() { doneCh <- struct{}{} }()
			return errors.New("normal error")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := q.Push(&Item{Topic: "p", DelaySecond: 1, Value: []byte("panic")}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "p", DelaySecond: 1, Value: []byte("err")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	<-doneCh
	// 等 monitor 计数刷入
	waitUntil(t, 2000, func() bool {
		return atomic.LoadInt64(&panicCount) >= 1 && atomic.LoadInt64(&errCount) >= 2
	})

	// panic 应只有一次
	if c := atomic.LoadInt64(&panicCount); c != 1 {
		t.Fatalf("want panic=1 got=%d", c)
	}
	// error 包含 panic 路径（ERROR + handle_error）和普通 error，共两次
	if c := atomic.LoadInt64(&errCount); c < 2 {
		t.Fatalf("want error>=2 got=%d", c)
	}
}

func TestMetric_HandlePanic_ManualMode(t *testing.T) {
	var panicCount int64
	q := New(WithRetryTimes(0), WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		if metric == MetricHandlePanic {
			atomic.AddInt64(&panicCount, value)
		}
	}))
	defer q.Close()
	doneCh := make(chan struct{}, 1)
	if err := q.StartManualAck("mp", func(item *Item, ack Acker) {
		defer func() { doneCh <- struct{}{} }()
		panic("manual boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "mp", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	waitUntil(t, 2000, func() bool { return atomic.LoadInt64(&panicCount) >= 1 })
	if atomic.LoadInt64(&panicCount) != 1 {
		t.Fatalf("manual panic want=1 got=%d", panicCount)
	}
}

// ===== D18: handle_duration_ms 上报 =====

func TestMetric_HandleDuration_Reported(t *testing.T) {
	var observed []int64
	var mu sync.Mutex
	q := New(WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		if metric == MetricHandleDurationMs {
			mu.Lock()
			observed = append(observed, value)
			mu.Unlock()
		}
	}))
	defer q.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	if err := q.Start("d", func(item *Item) error {
		time.Sleep(120 * time.Millisecond)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "d", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, &wg, 5*time.Second)

	// duration 上报在 handler 之后
	waitUntil(t, 2000, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	if len(observed) < 1 {
		t.Fatal("duration should be observed at least once")
	}
	d := observed[0]
	if d < 100 || d > 1000 {
		t.Fatalf("duration ms out of range: %d", d)
	}
}

func TestMetric_HandleDuration_AlsoOnError(t *testing.T) {
	// 即便 handler 失败也应上报耗时
	var seen int32
	q := New(WithRetryTimes(0), WithMonitorCounter(func(metric string, _ int64, _ prometheus.Labels) {
		if metric == MetricHandleDurationMs {
			atomic.StoreInt32(&seen, 1)
		}
	}))
	defer q.Close()
	doneCh := make(chan struct{}, 1)
	if err := q.Start("derr", func(item *Item) error {
		defer func() { doneCh <- struct{}{} }()
		return errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "derr", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	waitUntil(t, 2000, func() bool { return atomic.LoadInt32(&seen) == 1 })
}

// ===== D19: in_flight Gauge / Status.InFlight =====

func TestMetric_InFlight_StatusReturnsBaseline(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("a", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	s := q.Status()
	if s.InFlight == nil {
		t.Fatal("InFlight map should not be nil")
	}
	if v := s.InFlight["a"]; v != 0 {
		t.Fatalf("idle in_flight should be 0, got %d", v)
	}
}

func TestMetric_InFlight_TracksHandler(t *testing.T) {
	q := New(WithMaxConcurrency(2))
	defer q.Close()

	startCh := make(chan struct{}, 4)
	releaseCh := make(chan struct{})
	if err := q.Start("if", func(item *Item) error {
		startCh <- struct{}{}
		<-releaseCh
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_ = q.Push(&Item{Topic: "if", DelaySecond: 1, Value: []byte{byte(i)}})
	}
	// 等 2 个 handler 启动（被 sem 限到 2）
	<-startCh
	<-startCh

	// 此时 in_flight 应为 2
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v := q.Status().InFlight["if"]; v == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v := q.Status().InFlight["if"]; v != 2 {
		t.Fatalf("want in_flight=2 got=%d", v)
	}

	close(releaseCh)
	// 等所有 handler 返回，inFlight 应回到 0
	waitUntil(t, 5000, func() bool { return q.Status().InFlight["if"] == 0 })
}

func TestMetric_InFlight_ExposedViaCollector(t *testing.T) {
	q := New(WithName("flightq"))
	defer q.Close()
	if err := q.Start("f", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}

	col := q.Collector()
	descCh := make(chan *prometheus.Desc, 4)
	col.Describe(descCh)
	close(descCh)
	var hasInFlight bool
	for d := range descCh {
		if strings.Contains(d.String(), "flightq_status_in_flight") {
			hasInFlight = true
		}
	}
	if !hasInFlight {
		t.Fatal("collector should describe in_flight metric")
	}

	mch := make(chan prometheus.Metric, 8)
	col.Collect(mch)
	close(mch)
	var inFlightSeen bool
	for m := range mch {
		if !strings.Contains(m.Desc().String(), "in_flight") {
			continue
		}
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatal(err)
		}
		inFlightSeen = true
	}
	if !inFlightSeen {
		t.Fatal("collect should emit in_flight metric")
	}
}

func TestMetric_InFlight_DecreasesOnPanic(t *testing.T) {
	q := New(WithRetryTimes(0))
	defer q.Close()

	doneCh := make(chan struct{}, 1)
	if err := q.Start("ifp", func(item *Item) error {
		defer func() { doneCh <- struct{}{} }()
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "ifp", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	// panic 后 in_flight 应清零
	waitUntil(t, 3000, func() bool { return q.Status().InFlight["ifp"] == 0 })
}

// ===== Metric 常量 =====

func TestMetric_Constants(t *testing.T) {
	cases := map[string]string{
		MetricProduce:          "delayq_produce",
		MetricProduceError:     "delayq_produce_error",
		MetricHandle:           "delayq_handle",
		MetricHandleError:      "delayq_handle_error",
		MetricHandlePanic:      "delayq_handle_panic",
		MetricHandleDurationMs: "delayq_handle_duration_ms",
		MetricPollError:        "delayq_poll_error",
		MetricReclaim:          "delayq_reclaim",
		MetricReclaimError:     "delayq_reclaim_error",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("constant %q != expected %q", got, want)
		}
	}
}

// ===== TopicQueue.InFlight 直接调用 =====

func TestTopicQueue_InFlight_Memory(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "tinfl")
	defer tp.Close()
	if v := tp.InFlight(); v != 0 {
		t.Fatalf("initial want=0 got=%d", v)
	}

	startCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	if err := tp.Start(func(item *Item) error {
		startCh <- struct{}{}
		<-releaseCh
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-startCh
	if v := tp.InFlight(); v != 1 {
		t.Fatalf("during handler want=1 got=%d", v)
	}
	close(releaseCh)
	waitUntil(t, 3000, func() bool { return tp.InFlight() == 0 })
}

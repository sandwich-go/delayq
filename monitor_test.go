package delayq

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestCollector_DescribeAndCollect 验证 prometheus Collector 协议实现
func TestCollector_DescribeAndCollect(t *testing.T) {
	q := New(WithName("testq"))
	defer q.Close()

	if err := q.Start("topic-x", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Start("topic-y", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// 推入不会立即消费的 item
	for i := 0; i < 2; i++ {
		if err := q.Push(&Item{Topic: "topic-x", DelaySecond: 3600, Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Push(&Item{Topic: "topic-y", DelaySecond: 3600, Value: []byte("y")}); err != nil {
		t.Fatal(err)
	}

	col := q.Collector()

	// Describe
	descCh := make(chan *prometheus.Desc, 4)
	col.Describe(descCh)
	close(descCh)
	var descs []string
	for d := range descCh {
		descs = append(descs, d.String())
	}
	if len(descs) == 0 {
		t.Fatal("describe should emit at least one desc")
	}
	var found bool
	for _, d := range descs {
		if strings.Contains(d, "testq_status_queue_length") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("desc should contain metric name, got %v", descs)
	}

	// Collect
	metricCh := make(chan prometheus.Metric, 8)
	col.Collect(metricCh)
	close(metricCh)
	got := map[string]float64{}
	for m := range metricCh {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatal(err)
		}
		var queueLabel string
		for _, lp := range pb.GetLabel() {
			if lp.GetName() == "queue" {
				queueLabel = lp.GetValue()
			}
		}
		got[queueLabel] = pb.GetGauge().GetValue()
	}
	if got["topic-x"] != 2 {
		t.Fatalf("topic-x want=2 got=%v", got["topic-x"])
	}
	if got["topic-y"] != 1 {
		t.Fatalf("topic-y want=1 got=%v", got["topic-y"])
	}
}

// TestCollector_RegisterWithPrometheus 验证可与 prometheus.Registry 集成
func TestCollector_RegisterWithPrometheus(t *testing.T) {
	q := New(WithName("reg"))
	defer q.Close()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(q.Collector()); err != nil {
		t.Fatal(err)
	}
	// Gather 一次，确保 Describe/Collect 没有违反协议
	if _, err := reg.Gather(); err != nil {
		t.Fatal(err)
	}
}

// TestMonitorCount_NilCounter 验证 MonitorCounter 为 nil 时直接跳过，不 panic
func TestMonitorCount_NilCounter(t *testing.T) {
	opts := newConfig(WithMonitorCounter(nil))
	// 直接调用 monitorCount，应该直接返回不 panic
	monitorCount("m", "t", opts)
	monitorCount("m", "t", opts, 5)
}

// TestMonitorCount_WithValues 验证传入 values 的计数语义
func TestMonitorCount_WithValues(t *testing.T) {
	var total int64
	opts := newConfig(WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		atomic.AddInt64(&total, value)
	}))
	monitorCount("m", "t", opts)      // +1
	monitorCount("m", "t", opts, 5)   // +5
	monitorCount("m", "t", opts, 10)  // +10
	if total != 16 {
		t.Fatalf("want total=16 got=%d", total)
	}
}

// 空结构体保证 sync 引用以免 lint 报 unused
var _ = sync.Mutex{}

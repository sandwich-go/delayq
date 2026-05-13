package delayq

import (
	"github.com/prometheus/client_golang/prometheus"
)

const subsystem = "status"

// 公开的 metric 名称常量，便于业务侧路由 MonitorCounter 回调
const (
	// MetricProduce Push 成功 (Counter)
	MetricProduce = "delayq_produce"
	// MetricProduceError Push 失败 (Counter)
	MetricProduceError = "delayq_produce_error"
	// MetricHandle handler 处理成功 (Counter)
	MetricHandle = "delayq_handle"
	// MetricHandleError handler 返回 error (Counter)
	MetricHandleError = "delayq_handle_error"
	// MetricHandlePanic handler panic (Counter)
	MetricHandlePanic = "delayq_handle_panic"
	// MetricHandleDurationMs handler 执行耗时（毫秒，Histogram 风格上报）
	MetricHandleDurationMs = "delayq_handle_duration_ms"
	// MetricPollError Redis poll 失败 (Counter)
	MetricPollError = "delayq_poll_error"
	// MetricReclaim 一次 reclaim 搬运的 item 数 (Counter)
	MetricReclaim = "delayq_reclaim"
	// MetricReclaimError reclaim 失败 (Counter)
	MetricReclaimError = "delayq_reclaim_error"
	// MetricRateLimited Push 被限流拒绝 (Counter)
	MetricRateLimited = "delayq_rate_limited"
)

type statsGetter interface {
	Status() Status
}

// Collector 即 prometheus.Collector，便于用户复用
type Collector = prometheus.Collector

type statsCollector struct {
	getter          statsGetter
	queueLengthDesc *prometheus.Desc
	inFlightDesc    *prometheus.Desc
	opts            *Options
}

func newCollector(getter statsGetter, opts *Options) Collector {
	name := opts.GetName()
	return &statsCollector{
		getter: getter,
		opts:   opts,
		queueLengthDesc: prometheus.NewDesc(
			prometheus.BuildFQName(name, subsystem, "queue_length"),
			"Length of delay set per topic (waiting items).",
			[]string{"queue"},
			prometheus.Labels{},
		),
		inFlightDesc: prometheus.NewDesc(
			prometheus.BuildFQName(name, subsystem, "in_flight"),
			"Number of in-flight handler invocations per topic.",
			[]string{"queue"},
			prometheus.Labels{},
		),
	}
}

func (c statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queueLengthDesc
	ch <- c.inFlightDesc
}

func (c statsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.getter.Status()
	for k, v := range stats.QueueLength {
		ch <- prometheus.MustNewConstMetric(
			c.queueLengthDesc,
			prometheus.GaugeValue,
			float64(v),
			k,
		)
	}
	for k, v := range stats.InFlight {
		ch <- prometheus.MustNewConstMetric(
			c.inFlightDesc,
			prometheus.GaugeValue,
			float64(v),
			k,
		)
	}
}

// monitorCount 按 topic 上报一次计数；values[0] 为本次增量（默认 1）
func monitorCount(metric string, topic string, opts *Options, values ...int) {
	if opts.GetMonitorCounter() == nil {
		return
	}
	var value int64 = 1
	if len(values) > 0 {
		value = int64(values[0])
	}
	opts.GetMonitorCounter()(metric, value, map[string]string{"Queue": topic})
}

// monitorObserve 按 topic 上报一次观测值（如耗时毫秒数），用 int64 不损失精度
func monitorObserve(metric string, topic string, opts *Options, value int64) {
	if opts.GetMonitorCounter() == nil {
		return
	}
	opts.GetMonitorCounter()(metric, value, map[string]string{"Queue": topic})
}

func (q *baseQueue) monitorCount(metric string, values ...int) {
	monitorCount(metric, q.topic, q.opts, values...)
}

func (q *baseQueue) monitorObserve(metric string, value int64) {
	monitorObserve(metric, q.topic, q.opts, value)
}

// monitorCounter 由 queue 外观层调用，按 topic 上报计数（永远 +1）
func (q *queue) monitorCounter(metric, topic string) {
	monitorCount(metric, topic, q.opts)
}

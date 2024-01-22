package delayq

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystem = "status"
)

type statsGetter interface {
	Status() Status
}

type Collector = prometheus.Collector

type statsCollector struct {
	getter          statsGetter
	queueLengthDesc *prometheus.Desc
	opts            *Options
}

func newCollector(getter statsGetter, opts *Options) Collector {
	return &statsCollector{
		getter: getter,
		opts:   opts,
		queueLengthDesc: prometheus.NewDesc(
			prometheus.BuildFQName(opts.GetName(), subsystem, "queue_length"),
			"Length of topic queue.",
			[]string{"queue"},
			prometheus.Labels{},
		),
	}
}

func (c statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queueLengthDesc
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
}

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

func (q *baseQueue) monitorCount(metric string, values ...int) {
	monitorCount(metric, q.topic, q.opts, values...)
}
func (q *queue) monitorCounter(metric, topic string, values ...int) {
	monitorCount(metric, topic, q.opts, values...)
}

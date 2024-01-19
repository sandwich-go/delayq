package delayq

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
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

func monitorCount(monitors *sync.Map, metric string, topic string, opts *Options, values ...int) {
	builder := opts.GetMonitorBuilder()
	if builder == nil {
		return
	}
	counter, ok := monitors.Load(metric)
	if !ok {
		counter = builder.Build(metric, map[string]string{"Queue": topic})
		monitors.Store(metric, counter)
	}
	if len(values) == 0 {
		counter.(prometheus.Counter).Inc()
	} else {
		counter.(prometheus.Counter).Add(float64(values[0]))
	}
}

func (q *baseQueue) monitorCount(metric string, values ...int) {
	monitorCount(q.monitors, metric, q.topic, q.opts, values...)
}
func (q *queue) monitorCounter(metric, topic string, values ...int) {
	monitorCount(q.monitors, metric, topic, q.opts, values...)
}

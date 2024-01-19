package delayq

import (
	"fmt"
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

func monitorCount(m Monitor, metric string, value int64, labels prometheus.Labels) {
	if m == nil {
		return
	}
	err := m.Count(metric, value, labels)
	if err != nil {
		fmt.Println("monitor counter error", err)
	}
}

func (q *baseQueue) monitorCount(metric string, value int64, labels prometheus.Labels) {
	monitorCount(q.opts.GetMonitor(), metric, value, labels)
}

func (q *queue) monitorCount(metric string, value int64, labels prometheus.Labels) {
	monitorCount(q.opts.GetMonitor(), metric, value, labels)
}

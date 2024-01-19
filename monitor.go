package delayq

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandwich-go/logbus"
	monitor2 "github.com/sandwich-go/logbus/monitor"
)

const (
	subsystem = "status"
)

type monitor interface {
	Count(metric string, value int64, labels map[string]string)
}

type statsGetter interface {
	Status() Status
}

type statsCollector struct {
	getter          statsGetter
	queueLengthDesc *prometheus.Desc
	opts            *Options
}

func registerMonitor(getter statsGetter, opts *Options) monitor {
	collector := newCollector(getter, opts)
	if opts.GetMonitorEnable() {
		monitor2.RegisterCollector(collector)
	}
	return collector
}

func (c statsCollector) Count(metric string, value int64, labels map[string]string) {
	if !c.opts.GetMonitorEnable() {
		return
	}
	err := monitor2.Count(metric, value, labels)
	if err != nil {
		logbus.Error("monitor counter error", logbus.ErrorField(err))
	}
}

func newCollector(getter statsGetter, opts *Options) *statsCollector {
	return &statsCollector{
		getter: getter,
		opts:   opts,
		queueLengthDesc: prometheus.NewDesc(
			prometheus.BuildFQName(opts.GetMonitorNamespace(), subsystem, "queue_length"),
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

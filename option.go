package delayq

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//go:generate optionGen  --new_func=newConfig --option_return_previous=false
func OptionsOptionDeclareWithDefault() interface{} {
	return map[string]interface{}{
		// annotation@Name(comment="名称")
		"Name": "delayq",
		// annotation@Prefix(comment="前缀")
		"Prefix": "__dq",
		// annotation@RedisScriptBuilder(comment="redis 脚本工厂")
		"RedisScriptBuilder": RedisScriptBuilder(nil),
		// annotation@RetryTimes(comment="重试次数")
		"RetryTimes": 10,
		// annotation@OnDeadLetter(comment="当有死信")
		"OnDeadLetter": (func(item *Item))(nil),
		// annotation@MonitorCounter(comment="监控统计函数")
		"MonitorCounter": func(metric string, value int64, labels prometheus.Labels) {},
		// annotation@Logger(comment="日志实现，默认输出到 stderr")
		"Logger": Logger(nil),
		// annotation@MaxConcurrency(comment="单 topic 业务处理最大并发 goroutine 数，<=0 表示不限制")
		"MaxConcurrency": 256,
		// annotation@VisibilityTimeout(comment="Redis 队列：item 被 poll 拉走后多久未 ack 视为失败被 reclaim")
		"VisibilityTimeout": 10 * time.Minute,
		// annotation@RetryInterval(comment="基础重试间隔；下次重试时间 = now + RetryInterval * RetryBackoff^failedCount，且不超过 MaxRetryInterval")
		"RetryInterval": 1 * time.Second,
		// annotation@RetryBackoff(comment="重试退避系数，<=1 表示不退避（固定间隔）")
		"RetryBackoff": 1.0,
		// annotation@MaxRetryInterval(comment="重试间隔上限")
		"MaxRetryInterval": 60 * time.Second,
		// annotation@RetryIntervalFunc(comment="自定义重试间隔函数，传入失败次数（>=1），返回下次重试延迟；非 nil 时优先于 RetryInterval/Backoff")
		"RetryIntervalFunc": (func(failedCount int) time.Duration)(nil),
	}
}

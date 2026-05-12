package delayq

import (
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
	}
}

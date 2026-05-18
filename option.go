package delayq

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// OptionsOptionDeclareWithDefault 由 optiongen 工具读取以生成 Option 函数。
// 普通调用方不应直接使用此函数。
// OptionsOptionDeclareWithDefault
// 注释中的作用范围标记：
//   - [all]    内存队列与 Redis 队列均生效
//   - [redis]  仅 Redis 队列生效
//   - [memory] 仅内存队列生效
//
//go:generate optionGen  --new_func=newConfig --option_return_previous=false
func OptionsOptionDeclareWithDefault() interface{} {
	return map[string]interface{}{
		// annotation@MetricNamespace(comment="[all] Prometheus 指标 namespace；构造 metric FQN：<namespace>_status_<metric>")
		"MetricNamespace": "delayq",
		// annotation@RedisKeyPrefix(comment="[redis] Redis key 前缀；最终 key 形如 <prefix>:do:{<topic>}")
		"RedisKeyPrefix": "__dq",
		// annotation@RedisScriptBuilder(comment="[redis] Redis 脚本工厂；非 nil 时 New() 切换为 Redis 后端")
		"RedisScriptBuilder": RedisScriptBuilder(nil),
		// annotation@RetryTimes(comment="[all] 业务失败的最大重试次数；handler 失败次数达到该值后投递死信。0 = 不重试（首次失败即死信）；<0 = 无限重试")
		"RetryTimes": 10,
		// annotation@OnDeadLetter(comment="[all] 死信回调；nil 时仅打 WARN 日志")
		"OnDeadLetter": (func(item *Item))(nil),
		// annotation@MonitorCounter(comment="[all] 监控上报回调；同时承担计数与观测值上报")
		"MonitorCounter": func(metric string, value int64, labels prometheus.Labels) {},
		// annotation@Logger(comment="[all] 日志实现，nil 时使用默认 stderr logger")
		"Logger": Logger(nil),
		// annotation@MaxConcurrency(comment="[all] 单 topic 业务处理最大并发 goroutine 数；<=0 表示不限制")
		"MaxConcurrency": 256,
		// annotation@VisibilityTimeout(comment="[redis] item 被 poll 拉走后多久未 ack 视为失败被 reclaim")
		"VisibilityTimeout": 10 * time.Minute,
		// annotation@RetryInterval(comment="[all] 基础重试间隔；下次重试时间 = now + RetryInterval * RetryBackoff^(failedCount-1)，且不超过 MaxRetryInterval")
		"RetryInterval": 1 * time.Second,
		// annotation@RetryBackoff(comment="[all] 重试退避系数，<=1 表示不退避（固定间隔）")
		"RetryBackoff": 1.0,
		// annotation@MaxRetryInterval(comment="[all] 重试间隔上限")
		"MaxRetryInterval": 60 * time.Second,
		// annotation@RetryIntervalFunc(comment="[all] 自定义重试间隔函数，传入失败次数（>=1），返回下次重试延迟；非 nil 时优先于 RetryInterval/Backoff")
		"RetryIntervalFunc": (func(failedCount int) time.Duration)(nil),
		// annotation@DisableValueIndex(comment="[memory] 禁用 value->node 索引；启用后 Get/Cancel 不可用，但 Push 性能提升约 40%")
		"DisableValueIndex": false,
		// annotation@PushRatePerSec(comment="[all] Push 限流（每秒允许 token 数），<=0 表示不限流")
		"PushRatePerSec": float64(0),
		// annotation@PushBurst(comment="[all] Push 限流 burst 容量（token 数），<=0 时取 PushRatePerSec 同值")
		"PushBurst": int(0),
		// annotation@HeartbeatInterval(comment="[redis] handler 执行期间自动延期 doing 集 score 的心跳间隔；<0 表示禁用心跳；0 表示使用默认值 VisibilityTimeout/3（不少于 1s）")
		"HeartbeatInterval": time.Duration(0),
		// annotation@PollInterval(comment="[redis] 把 delay 集中到期 item 搬到 doing 集的轮询间隔；<=0 表示使用默认值 1s")
		"PollInterval": time.Duration(0),
		// annotation@ReclaimInterval(comment="[redis] 把 doing 集中超时 item 搬回 delay 集的轮询间隔；<=0 表示使用默认值 1s")
		"ReclaimInterval": time.Duration(0),
	}
}

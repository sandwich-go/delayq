package delayq

import "time"

// Preset 是一组 Option 的预设组合，用于降低使用门槛。
// 用法：
//
//	dq := delayq.New(append(delayq.HighThroughputPreset(),
//	    delayq.WithName("myapp"),       // 自定义 Option 放后面，会覆盖 preset 中相同 Option
//	)...)
//
// 多个 Preset 也可以叠加（同名 Option 后者生效）：
//
//	opts := append(delayq.HighThroughputPreset(), delayq.ReliablePreset()...)
//	dq := delayq.New(opts...)
type Preset = []Option

// HighThroughputPreset 高吞吐预设。
//
// 适合场景：每秒万级以上的 Push（订单消息、推送通知、日志缓冲）。
// 偏好：吞吐 > 延迟 > 单条精度。
//
// 包含：
//   - DisableValueIndex(true)        禁用 byValue 索引（Get/Cancel 不可用），Push 提速 ~40%
//   - MaxConcurrency(1024)           允许更高的 handler 并发
//   - RetryTimes(3)                  适度重试，避免热点 item 反复占用资源
//   - RetryInterval(2s) + Backoff 2x 指数退避避免重试风暴
//   - MaxRetryInterval(60s)          上限 60s
//   - VisibilityTimeout(5min)        Redis 模式下保留 5 分钟容错时间
//
// 注意：HighThroughput preset 禁用了 byValue 索引，调用 Get/Cancel 会返回 ErrValueIndexDisabled。
func HighThroughputPreset() Preset {
	return []Option{
		WithDisableValueIndex(true),
		WithMaxConcurrency(1024),
		WithRetryTimes(3),
		WithRetryInterval(2 * time.Second),
		WithRetryBackoff(2.0),
		WithMaxRetryInterval(60 * time.Second),
		WithVisibilityTimeout(5 * time.Minute),
	}
}

// LowLatencyPreset 低延迟预设。
//
// 适合场景：实时性要求高，希望 item 一旦到期就尽快派发（推送、IM 消息）。
// 偏好：延迟 > 吞吐。
//
// 包含：
//   - MaxConcurrency(256)            充足并发避免排队
//   - RetryTimes(2)                  少重试，失败快速进入死信
//   - RetryInterval(500ms)           短重试间隔
//   - RetryBackoff(1.0)              不退避（保持低延迟）
//   - MaxRetryInterval(2s)           即使失败也能很快重试完
//   - VisibilityTimeout(30s)         Redis 模式下短可见超时，崩溃后快速恢复
//
// 注意：高失败率场景下短间隔重试会放大错误率。建议配合 OnDeadLetter 路由到补偿系统。
func LowLatencyPreset() Preset {
	return []Option{
		WithMaxConcurrency(256),
		WithRetryTimes(2),
		WithRetryInterval(500 * time.Millisecond),
		WithRetryBackoff(1.0),
		WithMaxRetryInterval(2 * time.Second),
		WithVisibilityTimeout(30 * time.Second),
	}
}

// ReliablePreset 高可靠预设。
//
// 适合场景：每条消息都很重要，宁可重试到爆也不要丢（金融通知、对账消息）。
// 偏好：可靠性 > 性能。
//
// 包含：
//   - MaxConcurrency(64)             适度并发，避免压垮下游
//   - RetryTimes(15)                 大量重试
//   - RetryInterval(1s) + Backoff 2x 指数退避
//   - MaxRetryInterval(5min)         避免最终重试时间过长
//   - VisibilityTimeout(15min)       较长容错窗口，handler 慢也不会被重复派发
//
// 必须配合 OnDeadLetter 处理最终失败的消息（如写入死信队列或告警）。
func ReliablePreset() Preset {
	return []Option{
		WithMaxConcurrency(64),
		WithRetryTimes(15),
		WithRetryInterval(1 * time.Second),
		WithRetryBackoff(2.0),
		WithMaxRetryInterval(5 * time.Minute),
		WithVisibilityTimeout(15 * time.Minute),
	}
}

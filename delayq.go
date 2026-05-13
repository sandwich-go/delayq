// Package delayq 提供内存与 Redis 两种实现的延迟队列。
//
// 用法：
//
//	dq := delayq.New(delayq.WithMaxConcurrency(64))
//	defer dq.Close()
//	dq.Start("topic", func(item *delayq.Item) error { ... })
//	dq.Push(&delayq.Item{Topic: "topic", DelaySecond: 30, Value: []byte("payload")})
//
// 配置 WithRedisScriptBuilder(builder) 后切换到分布式 Redis 后端。
//
// 详细文档见 README.md。
package delayq

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// 公共错误
var (
	// ErrTopicQueueHasClosed topic 队列已关闭或不存在
	ErrTopicQueueHasClosed = errors.New("topic queue has closed")
	// ErrTopicQueueHasStarted topic 队列已经启动，不可重复 Start
	ErrTopicQueueHasStarted = errors.New("topic queue has started")
	// ErrTopicQueueHasRegistered 该 topic 已注册过同名队列
	ErrTopicQueueHasRegistered = errors.New("topic queue has registered")
	// ErrNilItem Push 时 item 为 nil
	ErrNilItem = errors.New("item is nil")
)

// Status 延迟队列汇总状态
type Status struct {
	// QueueLength 每个 topic 当前 delay 集中等待执行的 item 数（不含 doing 中的）
	QueueLength map[string]int64
}

// Queue 多 topic 延迟队列外观接口。通过 New 创建。
type Queue interface {
	// Status 获取队列当前状态
	Status() Status
	// Collector 获取 prometheus 的 Collector，可注册到 prometheus.Registry
	Collector() prometheus.Collector
	// Push 投递一条延迟任务，路由规则见 Queue.Push 实现说明
	Push(*Item) error
	// PushBatch 批量投递延迟任务，所有 item 必须属于同一 topic
	PushBatch([]*Item) error
	// Get 查询某个 value 在指定 topic 中是否存在以及剩余延迟
	Get(topic string, value []byte) (remaining time.Duration, exists bool, err error)
	// Cancel 取消指定 topic 中所有匹配 value 的 item
	Cancel(topic string, value []byte) (canceled bool, err error)
	// Start 启动指定主题的延迟队列；handler 返回 error 触发重试
	Start(topic string, f func(*Item) error) error
	// StartManualAck 启动指定主题的延迟队列（手动 ack 模式）；handler 必须显式 Ack/Nack
	StartManualAck(topic string, f func(*Item, Acker)) error
	// StartTopicQueue 启动一个外部构造的 TopicQueue（高级用法）
	StartTopicQueue(tq TopicQueue, f func(*Item) error) error
	// Stop 关闭指定主题的延迟队列；topic 不存在时返回 nil
	Stop(topic string) error
	// Close 关闭所有延迟队列
	Close() error
}

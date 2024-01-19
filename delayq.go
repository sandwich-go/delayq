package delayq

import "errors"

var (
	ErrTopicQueueHasClosed     = errors.New("topic queue has closed")
	ErrTopicQueueHasStarted    = errors.New("topic queue has started")
	ErrTopicQueueHasRegistered = errors.New("topic queue has registered")
)

// Status 延迟队列状态
type Status struct {
	QueueLength map[string]int64 // topic -> 队列长度
}

type Queue interface {
	// Status 获取队列当前状态
	Status() Status
	// Push 放入延迟任务
	Push(*Item) error
	// Start 启动指定主题的延迟队列
	Start(topic string, f func(*Item) error) error
	// StartTopicQueue 启动指定主题的延迟队列
	StartTopicQueue(tq TopicQueue, f func(*Item) error) error
	// Stop 关闭指定主题的延迟队列
	Stop(topic string) error
	// Close 关闭所有延迟队列
	Close() error
}

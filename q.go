package delayq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// errBatchTopicMismatch 批次中包含不同 topic 时返回
func errBatchTopicMismatch(want, got string, idx int) error {
	return fmt.Errorf("delayq: batch topic mismatch at items[%d]: want %q got %q", idx, want, got)
}

// TopicQueue 单 topic 队列接口
type TopicQueue interface {
	// Topic 返回该队列绑定的 topic 名
	Topic() string
	// Push 推送一条 item，按 Item.DelaySecond 延迟、Item.Priority 在同槽位内排序
	Push(*Item) error
	// PushBatch 批量推送
	PushBatch([]*Item) error
	// Length 返回 delay 集中等待执行的 item 数（不含 doing 中的）
	Length() int64
	// Get 查询 value 是否在队列中，返回剩余延迟（doing 中返回 0）。
	// 不区分 delay/doing 状态。
	Get(value []byte) (remaining time.Duration, exists bool, err error)
	// Cancel 取消队列中所有匹配 value 的 item，返回是否至少取消了一个。
	// 已经被 poll 拉走但尚未 ack 的 item 也会被尽量取消（doing 集），
	// 但若 handler 已开始执行则无法终止。
	Cancel(value []byte) (canceled bool, err error)
	// Start 启动该 topic 的消费 goroutine
	Start(func(item *Item) error) error
	// StartManualAck 启动手动 ack 模式：业务必须显式调用 Acker.Ack 或 Nack。
	// 与 Start 互斥，二选一。
	StartManualAck(func(item *Item, ack Acker)) error
	// Close 关闭队列，等待在途 handler 返回
	Close() error
}

type queue struct {
	ctx         context.Context
	cancel      context.CancelFunc
	opts        *Options
	topicQueues sync.Map
	monitors    *sync.Map
	collector   Collector

	mx sync.Mutex
}

// New 创建一个 Queue 外观对象。可通过 Options 配置 Redis 后端、并发上限、重试策略等。
//
// 不传任何 Option 时使用内存实现（默认 MaxConcurrency=256，VisibilityTimeout=10min）。
// 传入 WithRedisScriptBuilder 时自动切换为 Redis 实现。
func New(opts ...Option) Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &queue{opts: newConfig(opts...), ctx: ctx, cancel: cancel, monitors: new(sync.Map)}
	q.collector = newCollector(q, q.opts)
	return q
}

func (q *queue) Collector() Collector { return q.collector }

func (q *queue) Status() Status {
	q.mx.Lock()
	defer q.mx.Unlock()

	var s Status
	s.QueueLength = make(map[string]int64)
	q.topicQueues.Range(func(key, value any) bool {
		s.QueueLength[key.(string)] = value.(TopicQueue).Length()
		return true
	})
	return s
}

func (q *queue) StartTopicQueue(tq TopicQueue, f func(*Item) error) error {
	_, ok := q.topicQueues.LoadOrStore(tq.Topic(), tq)
	if ok {
		return ErrTopicQueueHasRegistered
	}
	return tq.Start(func(item *Item) error {
		err := f(item)
		if err != nil {
			q.monitorCounter("delayq_handle_error", tq.Topic())
		} else {
			q.monitorCounter("delayq_handle", tq.Topic())
		}
		return err
	})
}

func (q *queue) Start(topic string, f func(*Item) error) error {
	tq := q.newTopicQueue(topic)
	return q.StartTopicQueue(tq, f)
}

// StartManualAck 启动手动 ack 模式
func (q *queue) StartManualAck(topic string, f func(*Item, Acker)) error {
	tq := q.newTopicQueue(topic)
	_, ok := q.topicQueues.LoadOrStore(tq.Topic(), tq)
	if ok {
		return ErrTopicQueueHasRegistered
	}
	wrapped := func(item *Item, ack Acker) {
		f(item, ackerWithMonitor{
			inner: ack,
			topic: tq.Topic(),
			q:     q,
		})
	}
	return tq.StartManualAck(wrapped)
}

// newTopicQueue 根据 RedisScriptBuilder 是否设置选择 Redis 或 内存实现
func (q *queue) newTopicQueue(topic string) TopicQueue {
	if q.opts.GetRedisScriptBuilder() != nil {
		return newRedisTopicQueue(q.ctx, topic, q.opts)
	}
	return newMemoryTopicQueue(q.ctx, topic, q.opts)
}

// ackerWithMonitor 包装 Acker，让 Ack/Nack 也走 monitor 计数
type ackerWithMonitor struct {
	inner Acker
	topic string
	q     *queue
}

func (a ackerWithMonitor) Ack() {
	a.q.monitorCounter("delayq_handle", a.topic)
	a.inner.Ack()
}

func (a ackerWithMonitor) Nack(err error) {
	a.q.monitorCounter("delayq_handle_error", a.topic)
	a.inner.Nack(err)
}

func (q *queue) Stop(topic string) error {
	val, ok := q.topicQueues.Load(topic)
	if !ok {
		return nil
	}
	return val.(TopicQueue).Close()
}

func (q *queue) Close() error {
	q.mx.Lock()
	defer q.mx.Unlock()

	var err error
	q.topicQueues.Range(func(key, value any) bool {
		e := value.(TopicQueue).Close()
		if e != nil && err == nil {
			err = e
		}
		return true
	})
	q.cancel()
	return err
}

// Push 把 item 投递到对应 topic 的队列。
// 路由规则：
//  1. Item.Topic 非空 → 路由到该 topic
//  2. Item.Topic 为空 且 仅注册一个 topic → 自动路由到唯一 topic（并回填 Item.Topic）
//  3. Item.Topic 为空 且 注册了 0 或多个 topic → 返回 ErrTopicQueueHasClosed
func (q *queue) Push(item *Item) error {
	topic := item.GetTopic()
	if topic == "" {
		topic = q.resolveSingleTopic()
		if topic == "" {
			q.monitorCounter("delayq_produce_error", "")
			return ErrTopicQueueHasClosed
		}
		// 回填 Topic，方便后续 handler 访问
		item.Topic = topic
	}
	var err error
	val, ok := q.topicQueues.Load(topic)
	if !ok {
		err = ErrTopicQueueHasClosed
	} else {
		err = val.(TopicQueue).Push(item)
	}
	if err != nil {
		q.monitorCounter("delayq_produce_error", topic)
	} else {
		q.monitorCounter("delayq_produce", topic)
	}
	return err
}

// PushBatch 批量推送多个 item。所有 item 必须属于同一 topic
// （要么都填同一 Item.Topic；要么都为空且只注册了一个 topic）。
// 任意一个 item 的校验失败会导致整个批次返回错误（已经入队的不会回滚）。
func (q *queue) PushBatch(items []*Item) error {
	if len(items) == 0 {
		return nil
	}
	topic, err := q.resolveBatchTopic(items)
	if err != nil {
		q.monitorCounter("delayq_produce_error", topic)
		return err
	}
	val, ok := q.topicQueues.Load(topic)
	if !ok {
		q.monitorCounter("delayq_produce_error", topic)
		return ErrTopicQueueHasClosed
	}
	tq, ok := val.(interface{ PushBatch([]*Item) error })
	if !ok {
		// 后备：逐个 Push
		for _, it := range items {
			if e := val.(TopicQueue).Push(it); e != nil {
				q.monitorCounter("delayq_produce_error", topic)
				return e
			}
		}
	} else if e := tq.PushBatch(items); e != nil {
		q.monitorCounter("delayq_produce_error", topic)
		return e
	}
	for range items {
		q.monitorCounter("delayq_produce", topic)
	}
	return nil
}

// resolveBatchTopic 解析批次的目标 topic，并回填空 Item.Topic
func (q *queue) resolveBatchTopic(items []*Item) (string, error) {
	var topic string
	for i, it := range items {
		if it == nil {
			return "", ErrNilItem
		}
		t := it.GetTopic()
		if t == "" {
			if topic == "" {
				if only := q.resolveSingleTopic(); only != "" {
					topic = only
				}
			}
			if topic == "" {
				return "", ErrTopicQueueHasClosed
			}
			it.Topic = topic
			continue
		}
		if topic == "" {
			topic = t
		} else if topic != t {
			return "", errBatchTopicMismatch(topic, t, i)
		}
	}
	if topic == "" {
		return "", ErrTopicQueueHasClosed
	}
	return topic, nil
}

// Get 查询某个 value 是否存在于指定 topic 的队列中
func (q *queue) Get(topic string, value []byte) (time.Duration, bool, error) {
	val, ok := q.topicQueues.Load(topic)
	if !ok {
		return 0, false, ErrTopicQueueHasClosed
	}
	return val.(TopicQueue).Get(value)
}

// Cancel 取消 topic 中匹配 value 的 item
func (q *queue) Cancel(topic string, value []byte) (bool, error) {
	val, ok := q.topicQueues.Load(topic)
	if !ok {
		return false, ErrTopicQueueHasClosed
	}
	return val.(TopicQueue).Cancel(value)
}

// resolveSingleTopic 在仅注册一个 topic 时返回该 topic 名，否则返回空串
func (q *queue) resolveSingleTopic() string {
	var only string
	count := 0
	q.topicQueues.Range(func(key, _ any) bool {
		count++
		if count > 1 {
			only = ""
			return false
		}
		only = key.(string)
		return true
	})
	if count == 1 {
		return only
	}
	return ""
}

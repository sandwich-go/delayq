package delayq

import (
	"context"
	"sync"
)

type TopicQueue interface {
	Topic() string
	Push(*Item) error
	Length() int64
	Start() error
	Close() error
}

type queue struct {
	ctx         context.Context
	cancel      context.CancelFunc
	opts        *Options
	topicQueues sync.Map

	mx sync.Mutex
}

func New(opts ...Option) Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &queue{opts: newConfig(opts...), ctx: ctx, cancel: cancel}
	return q
}

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

func (q *queue) Register(tq TopicQueue) error {
	_, ok := q.topicQueues.LoadOrStore(tq.Topic(), tq)
	if ok {
		return ErrTopicQueueHasRegistered
	}
	return tq.Start()
}

func (q *queue) Start(topic string, f func(*Item) error) error {
	var tq TopicQueue
	if q.opts.GetRedisScriptBuilder() != nil {
		tq = newRedisTopicQueue(q.ctx, topic, f, q.opts)
	} else {
		tq = newMemoryTopicQueue(q.ctx, topic, f, q.opts)
	}
	return q.Register(tq)
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

func (q *queue) Push(item *Item) error {
	val, ok := q.topicQueues.Load(item.GetTopic())
	if !ok {
		return ErrTopicQueueHasClosed
	}
	return val.(TopicQueue).Push(item)
}

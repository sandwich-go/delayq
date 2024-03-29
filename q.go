package delayq

import (
	"context"
	"sync"
)

type TopicQueue interface {
	Topic() string
	Push(*Item) error
	Length() int64
	Start(func(item *Item) error) error
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
	var tq TopicQueue
	if q.opts.GetRedisScriptBuilder() != nil {
		tq = newRedisTopicQueue(q.ctx, topic, q.opts)
	} else {
		tq = newMemoryTopicQueue(q.ctx, topic, q.opts)
	}
	return q.StartTopicQueue(tq, f)
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
	var err error
	val, ok := q.topicQueues.Load(item.GetTopic())
	if !ok {
		err = ErrTopicQueueHasClosed
	} else {
		err = val.(TopicQueue).Push(item)
	}
	if err != nil {
		q.monitorCounter("delayq_produce_error", item.GetTopic())
	} else {
		q.monitorCounter("delayq_produce", item.GetTopic())
	}
	return err
}

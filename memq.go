package delayq

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

const wheelSize = 3600

type wheelNode struct {
	Id         string
	CycleCount int
	WheelIndex int
	Item       *Item
	Next       *wheelNode
}

type wheel struct {
	Nodes *wheelNode
}

type ticker struct {
	d time.Duration
	f func() error
}

type safeHandleItemFunc func(*Item) error

func (s safeHandleItemFunc) call(item *Item) error {
	if s == nil {
		return nil
	}
	return s(item)
}

type baseQueue struct {
	ctx      context.Context
	topic    string
	opts     *Options
	handle   safeHandleItemFunc
	failed   safeHandleItemFunc
	success  safeHandleItemFunc
	monitors *sync.Map

	wg      sync.WaitGroup
	exitC   chan struct{}
	started atomicInt32
}

func newBaseQueue(ctx context.Context, topic string, opts *Options) *baseQueue {
	return &baseQueue{
		ctx:      ctx,
		opts:     opts,
		topic:    topic,
		monitors: new(sync.Map),
	}
}

func (q *baseQueue) Topic() string { return q.topic }

func (q *baseQueue) executeOne(item *Item) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handle panic: %v", r)
			return
		}
	}()
	err = q.handle.call(item)
	return
}

func (q *baseQueue) executeOneWithRetry(item *Item) {
	err := q.executeOne(item)
	if err != nil {
		err = q.failed.call(item)
	} else {
		err = q.success.call(item)
	}
	if err != nil {
		// 输出日志
		fmt.Println("execute error", err, item)
	}
}

func (q *baseQueue) execute(items ...*Item) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		go func(j *Item) { q.executeOneWithRetry(j) }(item)
	}
}

func (q *baseQueue) close() error {
	if !q.started.CompareAndSwap(1, 0) {
		return ErrTopicQueueHasClosed
	}
	close(q.exitC)
	q.wg.Wait()
	return nil
}

func (q *baseQueue) isClosed() bool { return q.started.Get() == 0 }

func (q *baseQueue) start(f func(item *Item) error, ts ...ticker) error {
	if !q.started.CompareAndSwap(0, 1) {
		return ErrTopicQueueHasStarted
	}
	q.wg.Add(len(ts))
	q.handle = f
	q.exitC = make(chan struct{})
	var doTicker = func(ti ticker) {
		t := time.NewTimer(0)
		defer func() {
			_ = t.Stop()
			q.wg.Done()
		}()
		for {
			select {
			case <-t.C:
				_ = t.Reset(ti.d)
				if err := ti.f(); err != nil {
					// 输出日志
					fmt.Println("ticker error", err)
				}
			case <-q.exitC:
				return
			case <-q.ctx.Done():
				q.started.Set(0)
				return
			}
		}
	}
	for _, ti := range ts {
		go func(_ti ticker) {
			doTicker(_ti)
		}(ti)
	}
	return nil
}

type memQueue struct {
	*baseQueue
	index  int
	wheels [wheelSize]wheel

	mx    sync.Mutex
	query map[string]int
}

func NewMemoryTopicQueue(ctx context.Context, topic string, opts ...Option) TopicQueue {
	return newMemoryTopicQueue(ctx, topic, newConfig(opts...))
}

func newMemoryTopicQueue(ctx context.Context, topic string, opts *Options) TopicQueue {
	q := &memQueue{query: make(map[string]int)}
	q.baseQueue = newBaseQueue(ctx, topic, opts)
	q.baseQueue.failed = q.onFailed
	return q
}

func (q *memQueue) onFailed(item *Item) error {
	if item.GetDelaySecond() > 0 {
		item.DelaySecond = 0
	}
	item.DelaySecond--
	if int(math.Abs(float64(item.DelaySecond))) > q.opts.GetRetryTimes() {
		if f := q.opts.GetOnDeadLetter(); f != nil {
			f(item)
		}
		return nil
	}
	return q.Push(item)
}

func (q *memQueue) ticker() error {
	if q.index >= wheelSize {
		q.index = q.index % wheelSize
	}
	head := q.wheels[q.index].Nodes
	headIndex := q.index
	q.index++

	prev := head
	p := head
	for p != nil {
		if p.CycleCount == 0 {
			taskId := p.Id
			q.execute(p.Item)
			if prev == p {
				q.wheels[headIndex].Nodes = p.Next
				prev = p.Next
				p = p.Next
			} else {
				prev.Next = p.Next
				p = p.Next
			}
			q.mx.Lock()
			delete(q.query, taskId)
			q.mx.Unlock()
		} else {
			p.CycleCount--
			prev = p
			p = p.Next
		}
	}
	return nil
}

func (q *memQueue) Start(f func(item *Item) error) error {
	return q.start(f, ticker{d: 1 * time.Second, f: q.ticker})
}

func (q *memQueue) Length() int64 {
	q.mx.Lock()
	defer q.mx.Unlock()
	return int64(len(q.query))
}

func (q *memQueue) Close() error { return q.close() }

func (q *memQueue) Push(item *Item) error {
	if q.isClosed() {
		return ErrTopicQueueHasClosed
	}
	delaySecond := item.GetDelaySecond()
	if delaySecond < 0 {
		delaySecond = 0
	}
	seconds := delaySecond
	calculateValue := int64(q.index) + seconds

	cycle := int(calculateValue / wheelSize)
	index := int(calculateValue % wheelSize)

	n := &wheelNode{
		CycleCount: cycle,
		WheelIndex: index,
		Item:       item,
	}
	if cycle > 0 && index <= q.index {
		cycle--
		n.CycleCount = cycle
	}
	q.mx.Lock()
	if q.wheels[index].Nodes == nil {
		q.wheels[index].Nodes = n
	} else {
		head := q.wheels[index].Nodes
		n.Next = head
		q.wheels[index].Nodes = n
	}
	q.query[n.Id] = n.WheelIndex
	q.mx.Unlock()
	return nil
}

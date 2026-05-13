package delayq

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const wheelSize = 3600

type wheelNode struct {
	id         string
	cycleCount int
	wheelIndex int
	item       *Item
	next       *wheelNode
}

type wheel struct {
	nodes *wheelNode
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
	ctx     context.Context
	topic   string
	opts    *Options
	log     Logger
	handle  safeHandleItemFunc
	failed  safeHandleItemFunc
	success safeHandleItemFunc

	// wg 用于 ticker goroutine 的等待
	wg sync.WaitGroup
	// execWG 用于业务处理 goroutine 的等待，Close 时保证所有 handler 返回
	execWG sync.WaitGroup
	// sem worker pool 信号量，nil 表示不限制
	sem chan struct{}

	exitC     chan struct{}
	closeOnce sync.Once
	started   atomicInt32
}

func newBaseQueue(ctx context.Context, topic string, opts *Options) *baseQueue {
	q := &baseQueue{
		ctx:   ctx,
		opts:  opts,
		topic: topic,
		log:   opts.GetLogger(),
	}
	if q.log == nil {
		q.log = newDefaultLogger()
	}
	if n := opts.GetMaxConcurrency(); n > 0 {
		q.sem = make(chan struct{}, n)
	}
	return q
}

func (q *baseQueue) Topic() string { return q.topic }

func (q *baseQueue) executeOne(item *Item) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handle panic: %v", r)
			q.log.Errorf("topic=%s handle panic: %v", q.topic, r)
		}
	}()
	err = q.handle.call(item)
	return
}

func (q *baseQueue) executeOneWithRetry(item *Item) {
	err := q.executeOne(item)
	if err != nil {
		if ferr := q.failed.call(item); ferr != nil {
			q.log.Errorf("topic=%s failed callback error: %v item=%v", q.topic, ferr, item)
		}
	} else {
		if serr := q.success.call(item); serr != nil {
			q.log.Errorf("topic=%s success callback error: %v item=%v", q.topic, serr, item)
		}
	}
}

// execute 异步处理 items；受 MaxConcurrency 信号量约束，并通过 execWG 与 Close 同步
func (q *baseQueue) execute(items ...*Item) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		// 已关闭则不再派发
		if q.isClosed() {
			return
		}
		j := item
		q.execWG.Add(1)
		if q.sem != nil {
			// 阻塞式获取，若 Close 触发则退出
			select {
			case q.sem <- struct{}{}:
			case <-q.exitC:
				q.execWG.Done()
				return
			case <-q.ctx.Done():
				q.execWG.Done()
				return
			}
		}
		go func() {
			defer q.execWG.Done()
			if q.sem != nil {
				defer func() { <-q.sem }()
			}
			q.executeOneWithRetry(j)
		}()
	}
}

func (q *baseQueue) close() error {
	if !q.started.CompareAndSwap(1, 0) {
		return ErrTopicQueueHasClosed
	}
	q.closeOnce.Do(func() { close(q.exitC) })
	q.wg.Wait()
	// 等待所有在途业务 goroutine 返回，避免 handler 执行中队列已释放
	q.execWG.Wait()
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
	q.closeOnce = sync.Once{}
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
					q.log.Errorf("topic=%s ticker error: %v", q.topic, err)
				}
			case <-q.exitC:
				return
			case <-q.ctx.Done():
				// 仅触发状态变更与 exitC 关闭，让其他 ticker 也感知退出
				if q.started.CompareAndSwap(1, 0) {
					q.closeOnce.Do(func() { close(q.exitC) })
				}
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
	index int

	// mx 保护 wheels、query、index
	mx     sync.Mutex
	wheels [wheelSize]wheel
	query  map[string]struct{}

	// idSeq 为节点分配唯一 id
	idSeq uint64
}

func NewMemoryTopicQueue(ctx context.Context, topic string, opts ...Option) TopicQueue {
	return newMemoryTopicQueue(ctx, topic, newConfig(opts...))
}

func newMemoryTopicQueue(ctx context.Context, topic string, opts *Options) TopicQueue {
	q := &memQueue{query: make(map[string]struct{})}
	q.baseQueue = newBaseQueue(ctx, topic, opts)
	q.failed = q.onFailed
	return q
}

// onFailed 内存队列的失败回调
// 语义：Item.DelaySecond 为负时，其绝对值作为已失败次数。
// 达到 RetryTimes 投递死信，否则按 retry 策略计算延迟并重入队列。
func (q *memQueue) onFailed(item *Item) error {
	failedCount := 0
	if item.GetDelaySecond() < 0 {
		failedCount = int(-item.GetDelaySecond())
	}
	failedCount++
	if failedCount > q.opts.GetRetryTimes() {
		if f := q.opts.GetOnDeadLetter(); f != nil {
			f(item)
		} else {
			q.log.Warnf("topic=%s dead letter: %v", q.topic, item)
		}
		return nil
	}
	retry := &Item{
		Topic:       item.GetTopic(),
		DelaySecond: int64(-failedCount), // 负值编码失败次数
		Value:       item.GetValue(),
	}
	delay := computeRetryDelay(q.opts, failedCount)
	delaySec := int64(delay / time.Second)
	if delaySec < 1 {
		delaySec = 1 // 时间轮粒度为 1s，重试至少等下一个 tick
	}
	return q.pushRetry(retry, delaySec)
}

// pushRetry 按给定 delaySecond 重新入队，不修改 item.DelaySecond 中编码的失败计数
func (q *memQueue) pushRetry(item *Item, delaySecond int64) error {
	if q.isClosed() {
		return ErrTopicQueueHasClosed
	}
	if delaySecond < 0 {
		delaySecond = 0
	}
	q.mx.Lock()
	defer q.mx.Unlock()

	calculateValue := int64(q.index) + delaySecond
	cycle := int(calculateValue / wheelSize)
	idx := int(calculateValue % wheelSize)

	id := strconv.FormatUint(atomic.AddUint64(&q.idSeq, 1), 10)
	n := &wheelNode{
		id:         id,
		cycleCount: cycle,
		wheelIndex: idx,
		item:       item,
		next:       q.wheels[idx].nodes,
	}
	q.wheels[idx].nodes = n
	q.query[id] = struct{}{}
	return nil
}

// ticker 时间轮推进：检出当前槽位所有到期节点，批量派发给 execute
func (q *memQueue) ticker() error {
	q.mx.Lock()
	headIndex := q.index % wheelSize
	q.index = headIndex + 1

	// 使用 dummy head 简化链表删除
	dummy := &wheelNode{next: q.wheels[headIndex].nodes}
	prev := dummy
	var due []*Item
	for p := dummy.next; p != nil; {
		if p.cycleCount == 0 {
			// 取出并从链表中摘除
			due = append(due, p.item)
			delete(q.query, p.id)
			prev.next = p.next
			p = p.next
		} else {
			p.cycleCount--
			prev = p
			p = p.next
		}
	}
	q.wheels[headIndex].nodes = dummy.next
	q.mx.Unlock()

	if len(due) > 0 {
		q.execute(due...)
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

	q.mx.Lock()
	defer q.mx.Unlock()

	calculateValue := int64(q.index) + delaySecond
	cycle := int(calculateValue / wheelSize)
	idx := int(calculateValue % wheelSize)

	id := strconv.FormatUint(atomic.AddUint64(&q.idSeq, 1), 10)
	n := &wheelNode{
		id:         id,
		cycleCount: cycle,
		wheelIndex: idx,
		item:       item,
		next:       q.wheels[idx].nodes,
	}
	q.wheels[idx].nodes = n
	q.query[id] = struct{}{}
	return nil
}

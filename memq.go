package delayq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const wheelSize = 3600

type wheelNode struct {
	cycleCount int
	wheelIndex int
	priority   int32
	canceled   bool // Cancel 标记，ticker 时跳过
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
	ctx           context.Context
	topic         string
	opts          *Options
	log           Logger
	handle        safeHandleItemFunc
	failed        safeHandleItemFunc
	success       safeHandleItemFunc
	manualHandler func(*Item, Acker) // 非 nil 时启用手动 ack 模式
	// onItemStart 在 handler 即将执行前调用，返回 stop 函数；
	// stop 会在 handler 完成（含 panic / Acker.Ack/Nack）后被调用。
	// 用于 Redis 心跳延期等扩展。
	onItemStart func(*Item) (stop func())
	limiter     *tokenBucket // Push 限流器，nil 表示不限流

	// wg 用于 ticker goroutine 的等待
	wg sync.WaitGroup
	// execWG 用于业务处理 goroutine 的等待，Close 时保证所有 handler 返回
	execWG sync.WaitGroup
	// sem worker pool 信号量，nil 表示不限制
	sem chan struct{}
	// inFlight 当前正在执行 handler 的 goroutine 数（不含等待 sem 的）
	inFlight atomicInt64
	// pendingExec ticker 已检出但 execute 尚未启动 goroutine 的 item 数；
	// 用于 Drain 判定"全部消化"语义，避免 (count=0, inFlight=0) 的瞬时窗口
	pendingExec atomicInt64

	exitC     chan struct{}
	closeOnce sync.Once
	started   atomicInt32
	// draining 1 表示进入 drain 状态，拒绝新 Push 但允许现有 item 继续派发
	draining atomicInt32
}

// InFlight 返回当前正在执行 handler 的数量
func (q *baseQueue) InFlight() int64 { return q.inFlight.Get() }

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
	if rate := opts.GetPushRatePerSec(); rate > 0 {
		burst := opts.GetPushBurst()
		if burst <= 0 {
			burst = rate
		}
		q.limiter = newTokenBucket(rate, burst)
	}
	return q
}

func (q *baseQueue) Topic() string { return q.topic }

// largeValueWarnThreshold Push 时 Value 超过该字节数会记录 WARN 日志（不阻断）
const largeValueWarnThreshold = 4 * 1024

// drain 进入 drain 状态，拒绝新 Push 并等待所有 item 消化完毕。
// 由 TopicQueue 的 Drain 方法调用，需要 lengthFn 提供该队列的 Length 实现。
//
// 等待条件：lengthFn()==0 && pendingExec==0 && inFlight==0
//   - lengthFn  : 仍在 delay 集（内存：wheel）/doing 集中等待派发的 item
//   - pendingExec: ticker 已检出但 execute 尚未给 inFlight +1 的 item
//   - inFlight  : 已进入 handler 的 item
func (q *baseQueue) drain(ctx context.Context, lengthFn func() int64) error {
	if q.isClosed() {
		return ErrTopicQueueHasClosed
	}
	q.draining.Set(1)
	defer q.draining.Set(0)

	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		if lengthFn() == 0 && q.pendingExec.Get() == 0 && q.inFlight.Get() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.exitC:
			return ErrTopicQueueHasClosed
		case <-t.C:
		}
	}
}

// invokeDeadLetter 安全调用用户的 OnDeadLetter 回调，捕获 panic
func (q *baseQueue) invokeDeadLetter(item *Item) {
	f := q.opts.GetOnDeadLetter()
	if f == nil {
		q.log.Warnf("topic=%s dead letter: %v", q.topic, item)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			q.log.Errorf("topic=%s OnDeadLetter callback panic: %v item=%v", q.topic, r, item)
		}
	}()
	f(item)
}

// prepareItem 在 Push 前对 item 进行规范化与告警：
//   - 检查 drain 状态
//   - 检查限流（消耗 1 token）
//   - 注入 topic、校验 value 大小
//
// 返回 nil 表示通过；返回 error 表示拒绝入队。
func (q *baseQueue) prepareItem(item *Item) error {
	if item == nil {
		return ErrNilItem
	}
	if q.draining.Get() == 1 {
		return ErrDraining
	}
	if q.limiter != nil && !q.limiter.Allow() {
		q.monitorCount(MetricRateLimited)
		return ErrRateLimited
	}
	return q.normalizeItem(item)
}

// prepareItemNoRate 仅做 normalize，不检查 drain/限流（由调用方负责）。
// 用于 PushBatch 中已统一扣完 token 的场景。
func (q *baseQueue) prepareItemNoRate(item *Item) error {
	if item == nil {
		return ErrNilItem
	}
	return q.normalizeItem(item)
}

// normalizeItem 注入 topic 并对大 value 记 WARN
func (q *baseQueue) normalizeItem(item *Item) error {
	// 如果用户没填 Topic 或填错（非该 queue 的 topic），用 queue.topic 覆盖
	if item.GetTopic() != q.topic {
		if item.GetTopic() != "" {
			q.log.Debugf("topic=%s push item with mismatched topic %q, override", q.topic, item.GetTopic())
		}
		item.Topic = q.topic
	}
	if size := len(item.GetValue()); size >= largeValueWarnThreshold {
		q.log.Warnf("topic=%s pushing large item value: size=%d bytes (recommended <= 1KB; consider storing payload externally and pushing only an ID)",
			q.topic, size)
	}
	return nil
}

// executeOne 调用 handler 并区分 panic 与 error。
// panicked=true 表示 handler 抛出 panic（已被捕获），err 已包装成 error。
func (q *baseQueue) executeOne(item *Item) (err error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("handle panic: %v", r)
			q.log.Errorf("topic=%s handle panic: %v", q.topic, r)
		}
	}()
	err = q.handle.call(item)
	return
}

func (q *baseQueue) executeOneWithRetry(item *Item) {
	defer func() {
		if r := recover(); r != nil {
			q.log.Errorf("topic=%s ack callback panic: %v item=%v", q.topic, r, item)
		}
	}()
	// inFlight.Add(1) 已由 execute 同步执行，这里只负责 -1
	start := nowFunc()
	defer func() {
		q.inFlight.Add(-1)
		q.monitorObserve(MetricHandleDurationMs, nowFunc().Sub(start).Milliseconds())
	}()

	// onItemStart 钩子：用于 Redis 模式启动 heartbeat 等扩展
	var stopHook func()
	if q.onItemStart != nil {
		stopHook = q.onItemStart(item)
	}
	if stopHook != nil {
		defer stopHook()
	}

	// manual ack 模式：派发给用户回调 + Acker，由用户决定何时 ack
	if q.manualHandler != nil {
		acker := &itemAcker{q: q, item: item}
		// 用户回调本身的 panic 也要捕获，并视为 Nack
		func() {
			defer func() {
				if r := recover(); r != nil {
					q.log.Errorf("topic=%s manual handler panic: %v", q.topic, r)
					q.monitorCount(MetricHandlePanic)
					acker.Nack(fmt.Errorf("handler panic: %v", r))
				}
			}()
			q.manualHandler(item, acker)
		}()
		return
	}
	err, panicked := q.executeOne(item)
	if panicked {
		q.monitorCount(MetricHandlePanic)
	}
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

// execute 异步处理 items；受 MaxConcurrency 信号量约束，通过 execWG 与 Close 同步。
//
// 调度顺序：
//  1. 检查队列是否关闭，若关闭直接返回（剩余 items 丢弃）
//  2. 阻塞式获取信号量；若期间感知到关闭（exitC / ctx.Done）则退出
//  3. 信号量获取成功后 execWG.Add(1) + inFlight.Add(1)，保证 wg 严格成对
//  4. 子 goroutine 内 defer 释放信号量并 Done
func (q *baseQueue) execute(items ...*Item) {
	q.executeInternal(items, false)
}

// executeWithPending 与 execute 相同，但调用前 pendingExec 已被预加 +len(items)。
// 适用于 ticker 路径：避免 ticker 检出 item 后到子 goroutine 增加 inFlight 之间
// 出现 (Length=0, inFlight=0, pendingExec=0) 的瞬时窗口导致 Drain 早退。
func (q *baseQueue) executeWithPending(items ...*Item) {
	q.executeInternal(items, true)
}

func (q *baseQueue) executeInternal(items []*Item, hasPending bool) {
	if len(items) == 0 {
		return
	}
	useSem := q.sem != nil
	for i, item := range items {
		if q.isClosed() {
			if hasPending {
				q.pendingExec.Add(-int64(len(items) - i))
			}
			return
		}
		if useSem {
			select {
			case q.sem <- struct{}{}:
			case <-q.exitC:
				if hasPending {
					q.pendingExec.Add(-int64(len(items) - i))
				}
				return
			case <-q.ctx.Done():
				if hasPending {
					q.pendingExec.Add(-int64(len(items) - i))
				}
				return
			}
		}
		q.execWG.Add(1)
		// 同步增加 inFlight，避免子 goroutine 启动前被观察到 inFlight=0
		q.inFlight.Add(1)
		if hasPending {
			// item 已成功登记到 inFlight，可以从 pending 中移除
			q.pendingExec.Add(-1)
		}
		j := item
		go func() {
			defer q.execWG.Done()
			if useSem {
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

// tickerErrorBackoffMax ticker 连续失败时的退避上限
const tickerErrorBackoffMax = 30 * time.Second

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
		// 连续失败次数；每次成功重置为 0
		consecutiveErrs := 0
		for {
			select {
			case <-t.C:
				err := ti.f()
				next := ti.d
				if err != nil {
					q.log.Errorf("topic=%s ticker error: %v", q.topic, err)
					consecutiveErrs++
					if backoff := backoffDuration(ti.d, consecutiveErrs, tickerErrorBackoffMax); backoff > next {
						next = backoff
					}
				} else if consecutiveErrs > 0 {
					q.log.Infof("topic=%s ticker recovered after %d consecutive errors", q.topic, consecutiveErrs)
					consecutiveErrs = 0
				}
				_ = t.Reset(next)
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

// backoffDuration 计算 base * 2^(failures-1)，上限 max
func backoffDuration(base time.Duration, failures int, maxBackoff time.Duration) time.Duration {
	if failures <= 0 {
		return base
	}
	if base <= 0 {
		base = 1 * time.Second
	}
	// 防止移位溢出
	const maxShift = 30
	shift := failures - 1
	if shift > maxShift {
		shift = maxShift
	}
	d := base * (1 << shift)
	if maxBackoff > 0 && (d > maxBackoff || d < base) { // d<base 视为溢出
		return maxBackoff
	}
	return d
}

type memQueue struct {
	*baseQueue
	index int

	// mx 保护 wheels、count、byValue、index
	mx     sync.Mutex
	wheels [wheelSize]wheel
	// count 跟踪所有在队列中的节点数（含 canceled），用于 Length
	count int64
	// byValue 用于按 value 反查节点，支持 Get / Cancel；DisableValueIndex=true 时为 nil
	byValue map[string][]*wheelNode
}

// NewMemoryTopicQueue 构造一个仅在内存中的延迟队列。
// 时间轮粒度为 1 秒，最大延迟受 wheelSize（3600 秒）* cycle 约束（无硬上限）。
func NewMemoryTopicQueue(ctx context.Context, topic string, opts ...Option) TopicQueue {
	return newMemoryTopicQueue(ctx, topic, newConfig(opts...))
}

func newMemoryTopicQueue(ctx context.Context, topic string, opts *Options) TopicQueue {
	q := &memQueue{}
	if !opts.GetDisableValueIndex() {
		q.byValue = make(map[string][]*wheelNode)
	}
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
		q.invokeDeadLetter(item)
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
	q.insertLocked(item, delaySecond)
	return nil
}

// insertLocked 在持锁状态下把 item 插入到对应槽位，按 priority 降序保持链表有序
func (q *memQueue) insertLocked(item *Item, delaySecond int64) {
	calculateValue := int64(q.index) + delaySecond
	cycle := int(calculateValue / wheelSize)
	idx := int(calculateValue % wheelSize)

	n := &wheelNode{
		cycleCount: cycle,
		wheelIndex: idx,
		priority:   item.GetPriority(),
		item:       item,
	}
	// 按 priority 降序插入链表。
	// 当 head.priority <= n.priority 时直接头插：高优先级在前；
	// 同 priority 时新节点也插到队首（不保证 FIFO，但 push 路径 O(1)）。
	head := q.wheels[idx].nodes
	if head == nil || head.priority <= n.priority {
		n.next = head
		q.wheels[idx].nodes = n
	} else {
		prev := head
		for prev.next != nil && prev.next.priority > n.priority {
			prev = prev.next
		}
		n.next = prev.next
		prev.next = n
	}
	q.count++
	if q.byValue == nil {
		return
	}
	if v := item.GetValue(); len(v) != 0 {
		key := string(v)
		q.byValue[key] = append(q.byValue[key], n)
	}
}

// removeFromByValueLocked 在持锁下移除 byValue 索引中的节点
func (q *memQueue) removeFromByValueLocked(n *wheelNode) {
	if q.byValue == nil {
		return
	}
	v := string(n.item.GetValue())
	if v == "" {
		return
	}
	list := q.byValue[v]
	for i, p := range list {
		if p == n {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(q.byValue, v)
	} else {
		q.byValue[v] = list
	}
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
			if !p.canceled {
				due = append(due, p.item)
			}
			q.count--
			q.removeFromByValueLocked(p)
			prev.next = p.next
			p = p.next
		} else {
			p.cycleCount--
			prev = p
			p = p.next
		}
	}
	q.wheels[headIndex].nodes = dummy.next
	// 在持锁期间预加 pendingExec，避免 unlock → execute 之间出现
	// (count=0, inFlight=0, pendingExec=0) 的瞬时窗口导致 Drain 早退
	if n := len(due); n > 0 {
		q.pendingExec.Add(int64(n))
	}
	q.mx.Unlock()

	if len(due) > 0 {
		q.executeWithPending(due...)
	}
	return nil
}

func (q *memQueue) Start(f func(item *Item) error) error {
	return q.start(f, ticker{d: 1 * time.Second, f: q.ticker})
}

// StartManualAck 启动手动 ack 模式
func (q *memQueue) StartManualAck(f func(item *Item, ack Acker)) error {
	q.manualHandler = f
	return q.start(func(*Item) error { return nil }, ticker{d: 1 * time.Second, f: q.ticker})
}

func (q *memQueue) Length() int64 {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.count
}

func (q *memQueue) Close() error { return q.close() }

// Drain 进入 drain 状态：拒绝新 Push，等待所有现有 item 派发完毕（Length=0 && InFlight=0）。
// ctx 取消时提前返回 ctx.Err()。Drain 后队列仍可用（draining 标志被清除）。
func (q *memQueue) Drain(ctx context.Context) error {
	return q.drain(ctx, q.Length)
}

// Push 把 item 插入时间轮。同槽位中按 Priority 降序排列。
func (q *memQueue) Push(item *Item) error {
	if q.isClosed() {
		return ErrTopicQueueHasClosed
	}
	if err := q.prepareItem(item); err != nil {
		return err
	}
	delaySecond := item.GetDelaySecond()
	if delaySecond < 0 {
		delaySecond = 0
	}
	q.mx.Lock()
	defer q.mx.Unlock()
	q.insertLocked(item, delaySecond)
	return nil
}

// PushBatch 批量推送多个 item（持锁一次性插入全部）。
// 任何一个 item 校验失败都会终止批次（已入队的不会回滚）。
// 限流时整批一次性扣 len(items) 个 token，不足直接拒绝整批。
func (q *memQueue) PushBatch(items []*Item) error {
	if len(items) == 0 {
		return nil
	}
	if q.isClosed() {
		return ErrTopicQueueHasClosed
	}
	if q.draining.Get() == 1 {
		return ErrDraining
	}
	if q.limiter != nil && !q.limiter.AllowN(len(items)) {
		q.monitorCount(MetricRateLimited, len(items))
		return ErrRateLimited
	}
	// 此处 prepareItem 会被旁路（用 prepareItemNoRate）以避免重复扣 token
	for _, it := range items {
		if err := q.prepareItemNoRate(it); err != nil {
			return err
		}
	}
	q.mx.Lock()
	defer q.mx.Unlock()
	for _, it := range items {
		delaySecond := it.GetDelaySecond()
		if delaySecond < 0 {
			delaySecond = 0
		}
		q.insertLocked(it, delaySecond)
	}
	return nil
}

// Get 查询 value 是否存在于队列中。
// 多个相同 value 时返回剩余延迟最小的那个。
// DisableValueIndex=true 时返回 ErrValueIndexDisabled。
func (q *memQueue) Get(value []byte) (remaining time.Duration, exists bool, err error) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.byValue == nil {
		return 0, false, ErrValueIndexDisabled
	}
	list, ok := q.byValue[string(value)]
	if !ok || len(list) == 0 {
		return 0, false, nil
	}
	// 找到剩余延迟最小的节点
	minRemain := -1
	for _, n := range list {
		if n.canceled {
			continue
		}
		// remaining = (cycleCount * wheelSize + 距离 head 的偏移)
		offset := n.wheelIndex - q.index
		if offset < 0 {
			offset += wheelSize
		}
		remain := n.cycleCount*wheelSize + offset
		if minRemain < 0 || remain < minRemain {
			minRemain = remain
		}
	}
	if minRemain < 0 {
		return 0, false, nil
	}
	return time.Duration(minRemain) * time.Second, true, nil
}

// Cancel 标记所有匹配 value 的节点为 canceled，ticker 时跳过派发并清理。
// 返回是否至少取消了一个节点。
// DisableValueIndex=true 时返回 ErrValueIndexDisabled。
func (q *memQueue) Cancel(value []byte) (bool, error) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.byValue == nil {
		return false, ErrValueIndexDisabled
	}
	list, ok := q.byValue[string(value)]
	if !ok || len(list) == 0 {
		return false, nil
	}
	canceled := false
	for _, n := range list {
		if !n.canceled {
			n.canceled = true
			canceled = true
		}
	}
	return canceled, nil
}

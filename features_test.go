package delayq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ===== C12: PushBatch =====

// TestMemq_PushBatch 内存队列批量推送
func TestMemq_PushBatch(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "batch")
	defer tp.Close()

	var seen sync.Map
	var wg sync.WaitGroup
	wg.Add(5)
	if err := tp.Start(func(item *Item) error {
		seen.Store(string(item.GetValue()), true)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	items := make([]*Item, 5)
	for i := range items {
		items[i] = &Item{DelaySecond: 1, Value: []byte(fmt.Sprintf("item-%d", i))}
	}
	if err := tp.PushBatch(items); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 5 {
		t.Fatalf("want length=5 got=%d", l)
	}
	waitWithTimeout(t, &wg, 5*time.Second)
	for i := 0; i < 5; i++ {
		if _, ok := seen.Load(fmt.Sprintf("item-%d", i)); !ok {
			t.Errorf("item-%d not delivered", i)
		}
	}
}

// TestMemq_PushBatch_Empty 空切片不做任何操作
func TestMemq_PushBatch_Empty(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "empty-batch")
	defer tp.Close()
	if err := tp.PushBatch(nil); err != nil {
		t.Fatal(err)
	}
	if err := tp.PushBatch([]*Item{}); err != nil {
		t.Fatal(err)
	}
}

// TestRedisQueue_PushBatch Redis 批量推送
func TestRedisQueue_PushBatch(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "rbatch", WithRedisScriptBuilder(newTestBuilder(t)))
	defer tp.Close()

	items := []*Item{
		{Value: []byte("a")},
		{Value: []byte("b")},
		{Value: []byte("c")},
	}
	if err := tp.PushBatch(items); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 3 {
		t.Fatalf("want length=3 got=%d", l)
	}
}

// TestQueue_PushBatch_TopicMismatch Queue 层批次中 topic 不一致
func TestQueue_PushBatch_TopicMismatch(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("a", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := q.PushBatch([]*Item{
		{Topic: "a", Value: []byte("1")},
		{Topic: "b", Value: []byte("2")},
	})
	if err == nil || !contains(err.Error(), "batch topic mismatch") {
		t.Fatalf("want batch topic mismatch error, got %v", err)
	}
}

// TestQueue_PushBatch_AutoTopic 仅注册一个 topic 时 Item.Topic 为空自动注入
func TestQueue_PushBatch_AutoTopic(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("only", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	items := []*Item{
		{Value: []byte("1")},
		{Value: []byte("2")},
	}
	if err := q.PushBatch(items); err != nil {
		t.Fatal(err)
	}
	for i, it := range items {
		if it.GetTopic() != "only" {
			t.Errorf("item[%d] topic not injected: %q", i, it.GetTopic())
		}
	}
}

// ===== C14: Priority =====

// TestMemq_Priority_DispatchOrder 验证插入操作把不同 priority 的节点
// 在同一槽位中按降序排列。直接调用 insertLocked 避免 ticker 干扰。
func TestMemq_Priority_DispatchOrder(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "prio")
	mq := tp.(*memQueue)
	defer tp.Close()

	mq.mx.Lock()
	for _, p := range []int32{1, 5, 3, 10, 2} {
		mq.insertLocked(&Item{
			Topic:    "prio",
			Value:    []byte(fmt.Sprintf("p%d", p)),
			Priority: p,
		}, 1)
	}

	// 检查所有节点的 priority 在链表中按降序排列
	var prio []int32
	for i := 0; i < wheelSize; i++ {
		for n := mq.wheels[i].nodes; n != nil; n = n.next {
			prio = append(prio, n.priority)
		}
	}
	mq.mx.Unlock()
	if len(prio) != 5 {
		t.Fatalf("expected 5 nodes got %d: %v", len(prio), prio)
	}
	for i := 1; i < len(prio); i++ {
		if prio[i] > prio[i-1] {
			t.Fatalf("priority not desc-sorted: %v", prio)
		}
	}
}

// TestRedisQueue_Priority_ScoreEncoding Redis 同时间下高 priority 的 score 更小
// 由 ZSET ZRANGEBYSCORE 升序 → 高 priority 先被搬入 doing 集
func TestRedisQueue_Priority_ScoreEncoding(t *testing.T) {
	now := unix()
	scores := []float64{
		itemScore(now, 0),
		itemScore(now, 5),
		itemScore(now, 10),
	}
	if !sort.SliceIsSorted(scores, func(i, j int) bool { return scores[i] > scores[j] }) {
		t.Fatalf("higher priority should yield smaller score, got %v", scores)
	}
}

// TestItemScore_Encoding 验证 itemScore 的 priority 编码不影响秒级比较
func TestItemScore_Encoding(t *testing.T) {
	now := int64(1700000000)
	low := itemScore(now, 0)
	high := itemScore(now, 100)
	if !(high < low) {
		t.Fatalf("high priority should yield smaller score, low=%v high=%v", low, high)
	}
	// 高 priority 不应跨过下一秒
	nextSec := itemScore(now+1, 0)
	if !(low < nextSec) || !(high < nextSec) {
		t.Fatal("priority weight escaped second boundary")
	}
}

// ===== C15: Get =====

// TestMemq_Get_NotFound 不存在的 value 返回 false
func TestMemq_Get_NotFound(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "get-nf")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	d, ok, err := tp.Get([]byte("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("should not exist, got d=%v", d)
	}
}

// TestMemq_Get_Found 推入后立即查询有正确剩余延迟
func TestMemq_Get_Found(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "get-f")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 30, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d, ok, err := tp.Get([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should exist")
	}
	// 由于时间轮粒度 1s，剩余应在 [29s, 30s] 内
	if d < 29*time.Second || d > 31*time.Second {
		t.Fatalf("unexpected remaining: %v", d)
	}
}

// TestRedisQueue_Get Redis 实现 Get
func TestRedisQueue_Get(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "rget", WithRedisScriptBuilder(newTestBuilder(t)))
	defer tp.Close()

	if _, ok, _ := tp.Get([]byte("none")); ok {
		t.Fatal("should not exist before push")
	}
	if err := tp.Push(&Item{DelaySecond: 60, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	d, ok, err := tp.Get([]byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should exist after push")
	}
	if d < 58*time.Second || d > 62*time.Second {
		t.Fatalf("remaining out of range: %v", d)
	}
}

// TestQueue_Get_UnknownTopic Queue 层 Get 一个未注册 topic 返回 ErrTopicQueueHasClosed
func TestQueue_Get_UnknownTopic(t *testing.T) {
	q := New()
	defer q.Close()
	_, _, err := q.Get("nope", []byte("x"))
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// ===== C16: Cancel =====

// TestMemq_Cancel 取消后 ticker 不应派发
func TestMemq_Cancel(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "cancel")
	defer tp.Close()
	var calls int32
	if err := tp.Start(func(item *Item) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	canceled, err := tp.Cancel([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !canceled {
		t.Fatal("Cancel should return true")
	}
	// 等到原本会派发的时间过去
	time.Sleep(2200 * time.Millisecond)
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("handler should not be called after cancel, calls=%d", calls)
	}
	// 再次 cancel 同一 value 返回 false
	canceled2, _ := tp.Cancel([]byte("c"))
	if canceled2 {
		t.Fatal("second cancel should return false (no-op)")
	}
}

// TestMemq_Cancel_NotFound 不存在的 value
func TestMemq_Cancel_NotFound(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "c-nf")
	defer tp.Close()
	if err := tp.Start(func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	canceled, err := tp.Cancel([]byte("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if canceled {
		t.Fatal("Cancel of unknown value should return false")
	}
}

// TestRedisQueue_Cancel Redis Cancel
func TestRedisQueue_Cancel(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "rcancel", WithRedisScriptBuilder(newTestBuilder(t)))
	defer tp.Close()
	if err := tp.Push(&Item{DelaySecond: 60, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if l := tp.Length(); l != 1 {
		t.Fatalf("want 1 got %d", l)
	}
	canceled, err := tp.Cancel([]byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if !canceled {
		t.Fatal("Cancel should return true")
	}
	if l := tp.Length(); l != 0 {
		t.Fatalf("want 0 after cancel got %d", l)
	}
	// 再 cancel 一次返回 false
	canceled2, _ := tp.Cancel([]byte("v"))
	if canceled2 {
		t.Fatal("second cancel should return false")
	}
}

// TestQueue_Cancel_UnknownTopic
func TestQueue_Cancel_UnknownTopic(t *testing.T) {
	q := New()
	defer q.Close()
	_, err := q.Cancel("nope", []byte("x"))
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// ===== C13: Manual Ack =====

// TestMemq_StartManualAck 业务 Ack 后 item 不再重试
func TestMemq_StartManualAck(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "manual-ok", WithRetryTimes(3))
	defer tp.Close()

	var calls int32
	doneCh := make(chan struct{}, 1)
	if err := tp.StartManualAck(func(item *Item, ack Acker) {
		atomic.AddInt32(&calls, 1)
		ack.Ack()
		select {
		case doneCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	time.Sleep(1500 * time.Millisecond)
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Fatalf("after Ack handler should be called once, got %d", c)
	}
}

// TestMemq_StartManualAck_Nack Nack 触发重试
func TestMemq_StartManualAck_Nack(t *testing.T) {
	deadCh := make(chan struct{}, 1)
	tp := NewMemoryTopicQueue(context.Background(), "manual-nack",
		WithRetryTimes(2),
		WithRetryInterval(1*time.Second),
		WithOnDeadLetter(func(item *Item) {
			select {
			case deadCh <- struct{}{}:
			default:
			}
		}),
	)
	defer tp.Close()

	var calls int32
	if err := tp.StartManualAck(func(item *Item, ack Acker) {
		atomic.AddInt32(&calls, 1)
		ack.Nack(fmt.Errorf("oops"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deadCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout, calls=%d", atomic.LoadInt32(&calls))
	}
	if c := atomic.LoadInt32(&calls); c != 3 {
		t.Fatalf("want 3 attempts (1+2 retries), got %d", c)
	}
}

// TestAcker_DoubleCall 重复 Ack/Nack 不会引发副作用
func TestAcker_DoubleCall(t *testing.T) {
	tp := NewMemoryTopicQueue(context.Background(), "double-ack")
	defer tp.Close()

	doneCh := make(chan struct{}, 1)
	if err := tp.StartManualAck(func(item *Item, ack Acker) {
		ack.Ack()
		ack.Ack()                    // 应是 no-op
		ack.Nack(fmt.Errorf("late")) // 应是 no-op
		select {
		case doneCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{DelaySecond: 1, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	time.Sleep(1500 * time.Millisecond) // 确认没有触发额外的派发
	// 只要不 panic 就算通过
}

// TestRedisQueue_StartManualAck Redis 实现的手动 ack
func TestRedisQueue_StartManualAck(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "rmanual", WithRedisScriptBuilder(newTestBuilder(t)))
	defer tp.Close()

	doneCh := make(chan struct{}, 1)
	if err := tp.StartManualAck(func(item *Item, ack Acker) {
		ack.Ack()
		select {
		case doneCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Push(&Item{Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("redis manual ack timeout")
	}
}

// TestQueue_StartManualAck_Nack 验证 Queue 层 ackerWithMonitor.Nack 计数
func TestQueue_StartManualAck_Nack(t *testing.T) {
	var errCount int64
	q := New(WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		if metric == "delayq_handle_error" {
			atomic.AddInt64(&errCount, value)
		}
	}), WithRetryTimes(0))
	defer q.Close()

	doneCh := make(chan struct{}, 1)
	if err := q.StartManualAck("man-nack", func(item *Item, ack Acker) {
		ack.Nack(fmt.Errorf("boom"))
		select {
		case doneCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "man-nack", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	waitUntil(t, 2000, func() bool { return atomic.LoadInt64(&errCount) >= 1 })
}

// TestQueue_StartManualAck_Duplicate 重复 StartManualAck 同 topic 应失败
func TestQueue_StartManualAck_Duplicate(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.StartManualAck("dup-ma", func(item *Item, ack Acker) { ack.Ack() }); err != nil {
		t.Fatal(err)
	}
	if err := q.StartManualAck("dup-ma", func(item *Item, ack Acker) { ack.Ack() }); !errors.Is(err, ErrTopicQueueHasRegistered) {
		t.Fatalf("want ErrTopicQueueHasRegistered, got %v", err)
	}
}

// TestQueue_PushBatch_Empty 空切片
func TestQueue_PushBatch_Empty(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("e", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.PushBatch(nil); err != nil {
		t.Fatal(err)
	}
	if err := q.PushBatch([]*Item{}); err != nil {
		t.Fatal(err)
	}
}

// TestQueue_PushBatch_NilItem nil item 在 batch 中
func TestQueue_PushBatch_NilItem(t *testing.T) {
	q := New()
	defer q.Close()
	if err := q.Start("n", func(item *Item) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := q.PushBatch([]*Item{
		{Topic: "n", Value: []byte("ok")},
		nil,
	})
	if !errors.Is(err, ErrNilItem) {
		t.Fatalf("want ErrNilItem got %v", err)
	}
}

// TestQueue_PushBatch_UnknownTopic 批次 topic 未注册
func TestQueue_PushBatch_UnknownTopic(t *testing.T) {
	q := New()
	defer q.Close()
	err := q.PushBatch([]*Item{
		{Topic: "no", Value: []byte("x")},
	})
	if !errors.Is(err, ErrTopicQueueHasClosed) {
		t.Fatalf("want ErrTopicQueueHasClosed got %v", err)
	}
}

// TestParseFloat64_AllTypes
func TestParseFloat64_AllTypes(t *testing.T) {
	cases := []struct {
		in   interface{}
		want float64
	}{
		{float64(3.14), 3.14},
		{int64(42), 42},
		{int(42), 42},
		{"3.14", 3.14},
		{"abc", 0},
		{nil, 0},
	}
	for _, c := range cases {
		if got := parseFloat64(c.in); got != c.want {
			t.Errorf("parseFloat64(%v) want=%v got=%v", c.in, c.want, got)
		}
	}
}

// TestScoreToExecTs_Negative 负分数四舍五入
func TestScoreToExecTs_Negative(t *testing.T) {
	if scoreToExecTs(-1.5) != -2 {
		t.Fatal("negative score rounding wrong")
	}
	if scoreToExecTs(1.5) != 2 {
		t.Fatal("positive score rounding wrong")
	}
}

// TestQueue_StartManualAck Queue 外观层 + monitor 计数
func TestQueue_StartManualAck(t *testing.T) {
	var ackCount int64
	q := New(WithMonitorCounter(func(metric string, value int64, _ prometheus.Labels) {
		if metric == "delayq_handle" {
			atomic.AddInt64(&ackCount, value)
		}
	}))
	defer q.Close()

	doneCh := make(chan struct{}, 1)
	if err := q.StartManualAck("ma", func(item *Item, ack Acker) {
		ack.Ack()
		select {
		case doneCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&Item{Topic: "ma", DelaySecond: 1, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	<-doneCh
	// 等 metric 计数生效
	waitUntil(t, 2000, func() bool { return atomic.LoadInt64(&ackCount) >= 1 })
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

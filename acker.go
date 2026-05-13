package delayq

import "sync/atomic"

// Acker 用于手动 ack 模式下的应答。
// 每个 Acker 仅可调用一次 Ack 或 Nack，重复调用为 no-op。
//
// 用法（StartManualAck）：
//
//	dq.StartManualAck("topic", func(item *Item, ack Acker) {
//	    go func() {
//	        if err := process(item); err != nil {
//	            ack.Nack(err)
//	            return
//	        }
//	        ack.Ack()
//	    }()
//	})
//
// 注意：Acker 的回收依赖业务真的调用 Ack 或 Nack，否则 item 会留在 doing 集
// 直到 VisibilityTimeout 触发 reclaim 重新派发（可能导致重复处理）。
type Acker interface {
	// Ack 标记该 item 处理成功
	Ack()
	// Nack 标记该 item 处理失败，触发重试或死信
	Nack(err error)
}

// itemAcker 默认实现，复用 baseQueue 的 success/failed 回调链路
type itemAcker struct {
	q    *baseQueue
	item *Item
	done int32 // 0=未应答 1=已应答
}

func (a *itemAcker) Ack() {
	if !atomic.CompareAndSwapInt32(&a.done, 0, 1) {
		return
	}
	if err := a.q.success.call(a.item); err != nil {
		a.q.log.Errorf("topic=%s manual ack success error: %v", a.q.topic, err)
	}
}

func (a *itemAcker) Nack(err error) {
	if !atomic.CompareAndSwapInt32(&a.done, 0, 1) {
		return
	}
	if err != nil {
		a.q.log.Debugf("topic=%s manual nack: %v", a.q.topic, err)
	}
	if ferr := a.q.failed.call(a.item); ferr != nil {
		a.q.log.Errorf("topic=%s manual ack failed error: %v", a.q.topic, ferr)
	}
}

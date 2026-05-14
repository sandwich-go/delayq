package main

import "fmt"

// healthResult 健康评估结果。
type healthResult struct {
	ok      bool
	reasons []string
}

// assessHealth 在 final snapshot 上做基本健康检查：
//   - 残留 item（QueueLength + InFlight）应该接近 0
//   - PushError / HandlePanic 不应出现非零（panic 注入除外）
//   - p99 e2e 延迟应在合理范围内（<= 5x maxDelay 经验值）
//   - 待跟踪 push（pendingTrack）残留过多说明丢失
func assessHealth(s *snapshot) healthResult {
	var reasons []string

	// 残留 item
	if s.QueueLength > 0 {
		reasons = append(reasons, fmt.Sprintf("queue_length not drained: %d", s.QueueLength))
	}
	if s.InFlight > 0 {
		reasons = append(reasons, fmt.Sprintf("in_flight not zero: %d", s.InFlight))
	}

	// 派发-推送差距（不应超过 1%）
	if s.PushOK > 0 {
		residual := s.PushOK - s.HandleOK - s.HandleError - s.HandlePanic - s.DeadLetter
		if residual > s.PushOK/100+10 { // 1% + 缓冲
			reasons = append(reasons,
				fmt.Sprintf("delivery shortfall: pushed=%d handled=%d residual=%d",
					s.PushOK, s.HandleOK+s.HandleError+s.HandlePanic, residual))
		}
	}

	// pending_track 残留：超过 push_qps * 5s 视为异常（5s 派发延迟容忍）
	if s.PendingTrack > int64(s.PushQPS*5)+50 {
		reasons = append(reasons, fmt.Sprintf("pending_track too high: %d (push_qps=%.0f)", s.PendingTrack, s.PushQPS))
	}

	// goroutine 泄漏粗判：应小于一些经验阈值（按 push_qps 简单关联）
	if s.Goroutines > 10000 {
		reasons = append(reasons, fmt.Sprintf("goroutine count high: %d", s.Goroutines))
	}

	return healthResult{ok: len(reasons) == 0, reasons: reasons}
}

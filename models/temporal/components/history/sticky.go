package history

import (
	"github.com/law/GoDES/core"
)

// StickyQueue models Temporal's sticky-task queues.
//
// Sticky schedule-to-start: a sticky task that waits past s2sTimeoutMs
// falls back to the normal queue (the server's sticky S2S timeout —
// reference horizon 10s, DefaultWorkflowTaskTimeout at
// temporal/common/primitives/constants.go:16). The timeout is 0 by default
// (disabled — sticky tasks never S2S-expire, the pre-timeout behavior).
// Expiry is observed HEAD-ONLY at poll time (PollExpired), mirroring the
// matcher's residency-expiry-at-dispatch rule; it is a pure time
// comparison — zero RNG, zero sim mutation (the caller schedules the
// fallback re-add, exactly like Stick's fallback delay).
type StickyQueue struct {
	sim *core.Simulation

	stickyMs   int64
	fallbackMs int64
	// s2sTimeoutMs is the sticky schedule-to-start horizon (0 = disabled).
	s2sTimeoutMs int64

	buffers    map[uint64][]Task
	lastActive map[uint64]int64
	totalLen   int
}

// NewStickyQueue builds a sticky queue with no S2S expiry (s2sTimeoutMs =
// 0 — disabled, neutral: PollExpired never fires). Use
// NewStickyQueueWithS2S for the expiring variant.
func NewStickyQueue(sim *core.Simulation, stickyMs, fallbackMs int64) *StickyQueue {
	return NewStickyQueueWithS2S(sim, stickyMs, fallbackMs, 0)
}

// NewStickyQueueWithS2S builds a sticky queue whose head tasks expire via
// PollExpired once they wait past s2sTimeoutMs (<= 0 = disabled — identical
// to NewStickyQueue).
func NewStickyQueueWithS2S(sim *core.Simulation, stickyMs, fallbackMs, s2sTimeoutMs int64) *StickyQueue {
	return &StickyQueue{
		sim:          sim,
		stickyMs:     stickyMs,
		fallbackMs:   fallbackMs,
		s2sTimeoutMs: s2sTimeoutMs,
		buffers:      map[uint64][]Task{},
		lastActive:   map[uint64]int64{},
	}
}

// SetS2STimeoutMs sets the sticky schedule-to-start horizon (<= 0 =
// disabled). It is pure configuration — no RNG, no sim mutation.
func (s *StickyQueue) SetS2STimeoutMs(ms int64) { s.s2sTimeoutMs = ms }

// S2STimeoutMs returns the sticky schedule-to-start horizon (<= 0 =
// disabled).
func (s *StickyQueue) S2STimeoutMs() int64 { return s.s2sTimeoutMs }

// Stick places a workflow task on worker w's sticky buffer.
func (s *StickyQueue) Stick(w uint64, t Task, nowMs int64) (bool, int64) {
	last, ok := s.lastActive[w]
	if ok && s.stickyMs > 0 && nowMs-last > s.stickyMs {
		return false, s.fallbackMs
	}
	if !ok && s.stickyMs > 0 && s.stickyMs < nowMs {
		return false, s.fallbackMs
	}
	s.buffers[w] = append(s.buffers[w], t)
	s.totalLen++
	return true, 0
}

// Peek returns the oldest task on worker w's sticky buffer WITHOUT
// removing it (a drainer checks availability before committing a poller).
func (s *StickyQueue) Peek(w uint64) (Task, bool) {
	b := s.buffers[w]
	if len(b) == 0 {
		return Task{}, false
	}
	return b[0], true
}

// Poll takes the oldest task from worker w's sticky buffer.
func (s *StickyQueue) Poll(w uint64) (Task, bool) {
	b := s.buffers[w]
	if len(b) == 0 {
		return Task{}, false
	}
	t := b[0]
	s.buffers[w] = b[1:]
	s.totalLen--
	return t, true
}

// PollExpired expires worker w's HEAD task when it has waited out the
// sticky schedule-to-start horizon (nowMs − head.BornMs >= s2sTimeoutMs)
// and reports the fallback re-add delay — the sticky-to-normal fallback
// (a reschedule, never terminal: the workflow task is re-issued on the
// normal queue, unlike an activity S2S expiry). Only the head is observed
// per call; the caller loops. The task's BornMs is its stick time, so the
// age keeps counting across the wait.
//
// The disabled path (s2sTimeoutMs <= 0) is neutral: it reports no expiry
// and mutates nothing, so a default queue behaves exactly as before. A
// future-dated head (negative age) never expires. Pure time comparison:
// zero RNG, zero sim events — the caller schedules the fallback re-add
// from the returned delay, exactly like Stick's fallback path.
func (s *StickyQueue) PollExpired(w uint64, nowMs int64) (Task, int64, bool) {
	if s.s2sTimeoutMs <= 0 {
		return Task{}, 0, false
	}
	b := s.buffers[w]
	if len(b) == 0 {
		return Task{}, 0, false
	}
	if nowMs-b[0].BornMs < s.s2sTimeoutMs {
		return Task{}, 0, false
	}
	t := b[0]
	s.buffers[w] = b[1:]
	s.totalLen--
	return t, s.fallbackMs, true
}

// MarkActive records worker activity at nowMs.
func (s *StickyQueue) MarkActive(w uint64, nowMs int64) {
	s.lastActive[w] = nowMs
}

// Len returns the total sticky backlog.
func (s *StickyQueue) Len() int { return s.totalLen }

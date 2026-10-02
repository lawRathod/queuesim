package history

import (
	"math"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
)

// Task is an alias to the canonical task type.
type Task = matcher.Task

// HistoryShard models one Temporal history shard.
//
// Timer semantics: a timer fires only when its fire time has been reached
// (fireAtMs <= now) — timers NEVER fire early. timerShiftMs is a read-ahead
// horizon (how far ahead the shard may look when prefetching timer state),
// not an early-fire window: it has no effect on when Tick delivers timers.
//
// Batch semantics: the per-tick drain caps are the BATCH sizes
// (transferBatch, timerBatch) only. transferWorkers/timerWorkers are
// parallelism configuration carried for fidelity with the server's
// processor options — they have NO per-tick effect because the model
// drains serially on the single simulation goroutine.
type HistoryShard struct {
	sim *core.Simulation

	idx int

	persistMs        int64
	transferBatch    int
	transferWorkers  int
	timerBatch       int
	timerWorkers     int
	timerShiftMs     int64
	reschedBackoffMs int64

	transfer []Task
	timer    []timerEntry
	resched  []reschedEntry

	// History-boundary INTAKE counters (WP3): the cumulative offered load
	// on each queue kind, incremented by the enqueue methods below. Plain
	// struct fields (free, zero digest effect); they are registered with the
	// monitor ONLY under Config.LabeledMetrics. The offered-load RATE at the
	// history boundary is the delta of these over time.
	intakeTransfer uint64
	intakeTimer    uint64
	intakeResched  uint64

	lastWriteMs int64

	// persistBucket is the S4 persistence rate limiter shared by every shard
	// of a runtime (ADR-106 WP4). nil = off (the neutrality default — the
	// persistScaled path is untouched and byte-identical).
	persistBucket *PersistenceBucket
}

// PersistenceBucket is the S4 persistence rate limiter (ADR-106 WP4): a
// token bucket on the persistence-write dispatch path, mirroring the real
// server's per-service persistence limiter — the golang x/time/rate bucket
// (temporal/common/quotas/rate_limiter_impl.go:27, built from the dynamic
// rate/burst at temporal/common/quotas/dynamic.go:74-95). It is a pure
// deterministic function of sim time: lazy refill at admit time, zero RNG
// draws, zero wall-clock — bit-exact under a fixed seed.
//
// A write within the burst capacity is admitted immediately; once the burst
// is exhausted each write waits for its token (one per 1000/qps ms). The
// effective burst (QPS × PersistenceQPSBurstRatio) is derived HERE in code
// (ADR-106 decision 3, CODE-not-comment) — see NewPersistenceBucket.
// qps <= 0 = off: the bucket is inert and admits nothing (the neutrality
// default; the shard never consults it).
type PersistenceBucket struct {
	qps    float64 // token refill rate, tokens/s (the write ceiling); <= 0 = off
	burst  float64 // bucket capacity, tokens = int(qps × ratio), min 1
	tokens float64 // current level; may go negative (tokens reserved ahead)
	lastMs float64 // last refill anchor, sim ms
}

// NewPersistenceBucket builds the persistence token bucket from the S4
// config surface (maxQPS, burstRatio). This is the rate-limiter CONSTRUCTION
// call site in code: burst = int(maxQPS × burstRatio) with the server's
// min-1 rule, mirroring the real burst derivation
// (temporal/common/quotas/dynamic.go:78-92: `burst := int(rate * ratio)`, min
// 1 when both are positive; ratio default 1.0 at
// temporal/common/dynamicconfig/constants.go:251). A maxQPS <= 0 bucket is
// inert (off).
func NewPersistenceBucket(maxQPS, burstRatio float64) *PersistenceBucket {
	if maxQPS < 0 {
		maxQPS = 0
	}
	if burstRatio < 0 {
		burstRatio = 0
	}
	burst := float64(int(maxQPS * burstRatio))
	if burst == 0 && maxQPS > 0 && burstRatio > 0 {
		burst = 1
	}
	return &PersistenceBucket{qps: maxQPS, burst: burst, tokens: burst}
}

// admit reserves one persistence write arriving at sim time nowMs and
// returns its admission sim-ms (>= nowMs). Refill is lazy (golang
// x/time/rate reserve semantics on sim time): tokens accrue at qps/s up to
// the burst capacity, and a write past the burst waits until its token —
// the wait is the deficit ÷ qps, rounded UP (ceil) so the observed write
// rate never exceeds the configured ceiling on the integer-ms sim clock.
func (b *PersistenceBucket) admit(nowMs int64) int64 {
	now := float64(nowMs)
	if now > b.lastMs {
		b.tokens = math.Min(b.burst, b.tokens+(now-b.lastMs)*b.qps/1000.0)
		b.lastMs = now
	}
	b.tokens--
	if b.tokens >= 0 {
		return nowMs
	}
	return nowMs + int64(math.Ceil(-b.tokens*1000.0/b.qps))
}

type timerEntry struct {
	task     Task
	fireAtMs int64
}

type reschedEntry struct {
	task      Task
	retryAtMs int64
}

// NewHistoryShard builds a shard.
func NewHistoryShard(sim *core.Simulation, idx int, persistMs, transferBatch, transferWorkers, timerBatch, timerWorkers, timerShiftMs, reschedBackoffMs int64) *HistoryShard {
	s := &HistoryShard{
		sim:              sim,
		idx:              idx,
		persistMs:        persistMs,
		transferBatch:    int(transferBatch),
		transferWorkers:  int(transferWorkers),
		timerBatch:       int(timerBatch),
		timerWorkers:     int(timerWorkers),
		timerShiftMs:     timerShiftMs,
		reschedBackoffMs: reschedBackoffMs,
	}
	if s.transferBatch <= 0 {
		s.transferBatch = 1
	}
	if s.transferWorkers <= 0 {
		s.transferWorkers = 1
	}
	if s.timerBatch <= 0 {
		s.timerBatch = 1
	}
	if s.timerWorkers <= 0 {
		s.timerWorkers = 1
	}
	return s
}

// LenTransfer / LenTimer / LenResched report queue depths.
func (s *HistoryShard) LenTransfer() int { return len(s.transfer) }
func (s *HistoryShard) LenTimer() int    { return len(s.timer) }
func (s *HistoryShard) LenResched() int  { return len(s.resched) }

// IntakeTransfer / IntakeTimer / IntakeResched report the cumulative
// offered load on each history queue kind (WP3): the number of tasks ever
// enqueued on the transfer / timer / reschedule surface. Plain reads — no
// RNG, no mutation.
func (s *HistoryShard) IntakeTransfer() uint64 { return s.intakeTransfer }
func (s *HistoryShard) IntakeTimer() uint64    { return s.intakeTimer }
func (s *HistoryShard) IntakeResched() uint64  { return s.intakeResched }

// LastWriteMs returns the shard's persistence cursor.
func (s *HistoryShard) LastWriteMs() int64 { return s.lastWriteMs }

// SetPersistenceBucket attaches the S4 persistence rate limiter to the
// shard. One bucket is shared across all shards of a runtime: the real
// server builds a single per-service persistence limiter
// (service/history/fx.go:293 → common/persistence/client/fx.go:117-138), so
// the model's ceiling is service-wide, not per-shard. nil (or an inert
// zero-rate bucket) = off = today's unbounded path. Deterministic: the
// bucket's only caller is persistScaled, in single-goroutine sim order.
func (s *HistoryShard) SetPersistenceBucket(b *PersistenceBucket) {
	s.persistBucket = b
}

// EnqueueTransfer buffers a task on the shard's transfer queue. It is the
// history-boundary INTAKE chokepoint: every production offer of a task to
// history routes through here (matching_add/matching_offer/partition/
// matching_scale/matching/history), so the intakeTransfer counter is the
// offered-load rate at that boundary (WP3).
func (s *HistoryShard) EnqueueTransfer(t Task) bool {
	s.intakeTransfer++
	s.transfer = append(s.transfer, t)
	return true
}

// AddTimer schedules a timer event at absolute fireAtMs (timer intake).
func (s *HistoryShard) AddTimer(t Task, fireAtMs int64) {
	s.intakeTimer++
	s.timer = append(s.timer, timerEntry{task: t, fireAtMs: fireAtMs})
}

// Reschedule re-queues a failed execution for retry after the backoff
// (rescheduler intake).
func (s *HistoryShard) Reschedule(t Task, nowMs int64) {
	s.intakeResched++
	s.resched = append(s.resched, reschedEntry{task: t, retryAtMs: nowMs + s.reschedBackoffMs})
}

// RescheduleAt re-queues a failed execution for retry at an explicit time
// (rescheduler intake).
func (s *HistoryShard) RescheduleAt(t Task, retryAtMs int64) {
	s.intakeResched++
	s.resched = append(s.resched, reschedEntry{task: t, retryAtMs: retryAtMs})
}

// Persist simulates one serialized persistence write. When the S4
// persistence rate limiter is attached (NewPersistenceBucket with maxQPS >
// 0), the write's ADMISSION is additionally paced by the shared bucket's
// ceiling (QPS, burst = QPS × ratio) before the latency below is applied.
func (s *HistoryShard) Persist(now core.SimTime) core.SimTime {
	return s.persistScaled(now, 1)
}

// PersistScaled is Persist with a latency multiplier.
func (s *HistoryShard) PersistScaled(now core.SimTime, mult float64) core.SimTime {
	return s.persistScaled(now, mult)
}

func (s *HistoryShard) persistScaled(now core.SimTime, mult float64) core.SimTime {
	nowMs := now.Int64()
	// S4 persistence QPS ceiling: pace the write's admission by the shared
	// token bucket before serialization. Skipped entirely when the limiter
	// is off (nil or zero-rate) — the unbounded path is byte-identical.
	if s.persistBucket != nil && s.persistBucket.qps > 0 {
		nowMs = s.persistBucket.admit(nowMs)
	}
	if nowMs < s.lastWriteMs {
		nowMs = s.lastWriteMs
	}
	latency := float64(s.persistMs)
	if mult > 0 {
		latency *= mult
	}
	at := core.SimTime(nowMs).Add(int64(latency))
	s.lastWriteMs = at.Int64()
	return at
}

// Tick advances the shard's processors: due timers fire (never early —
// only entries with fireAtMs <= now), up to transferBatch transfers
// drain, and due reschedules route to onResched when provided (the
// scheduler hook — the reschedule-to-scheduler path) or fall back to the
// transfer queue when omitted (the legacy path, preserving the wiring of
// Tick call sites that have not yet passed the hook). The per-tick caps
// are the batch sizes only — the worker counts have no per-tick effect.
// A nil callback is skipped (the queue still advances). Slice order is
// dispatch order — deterministic.
func (s *HistoryShard) Tick(now core.SimTime, onTransfer func(Task), onTimer func(Task), onResched ...func(Task)) {
	nowMs := now.Int64()
	fired := 0
	snapshot := append([]timerEntry(nil), s.timer...)
	remaining := make([]timerEntry, 0, len(s.timer))
	for _, e := range snapshot {
		if e.fireAtMs <= nowMs && fired < s.timerBatch {
			fired++
			if onTimer != nil {
				onTimer(e.task)
			}
			continue
		}
		remaining = append(remaining, e)
	}
	if len(s.timer) > len(snapshot) {
		remaining = append(remaining, s.timer[len(snapshot):]...)
	}
	s.timer = remaining
	for i := 0; i < s.transferBatch && len(s.transfer) > 0; i++ {
		t := s.transfer[0]
		s.transfer = s.transfer[1:]
		if onTransfer != nil {
			onTransfer(t)
		}
	}
	var dueResched []reschedEntry
	remainingResched := make([]reschedEntry, 0, len(s.resched))
	for _, e := range s.resched {
		if e.retryAtMs <= nowMs {
			dueResched = append(dueResched, e)
		} else {
			remainingResched = append(remainingResched, e)
		}
	}
	s.resched = remainingResched
	var route func(Task)
	if len(onResched) > 0 {
		route = onResched[0]
	}
	for _, e := range dueResched {
		if route != nil {
			route(e.task)
			continue
		}
		s.transfer = append(s.transfer, e.task)
	}
}

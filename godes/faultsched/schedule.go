package faultsched

import (
	"fmt"
	"math"
	"sort"
)

// Kind identifies a fault kind in a schedule. The 12 existing model kinds
// are plain strings matching model.VectorKind values (ADR-100: no model
// import — the model leg maps schedule kinds onto its faults.Target
// dispatcher through a thin adapter). The three trailing kinds are
// RESERVED: they validate and round-trip but carry no leg behavior yet.
type Kind string

const (
	// KindAddDrop drops a fraction of tasks entering the matching queue
	// (transport-level AddTask drop). Rate = probability.
	KindAddDrop Kind = "add_drop"
	// KindWfDrop drops a fraction of wf-class tasks at the matching-wf
	// queue during the window. Rate = probability.
	KindWfDrop Kind = "wf_drop"
	// KindPollDrop fails a fraction of poller polls (no dispatch that
	// poll). Rate = probability.
	KindPollDrop Kind = "poll_drop"
	// KindActivityFailure makes activity attempts fail during the window.
	// Rate = scalar (probability per attempt is leg-local).
	KindActivityFailure Kind = "activity_failure"
	// KindS2STimeout sets the matching queues' schedule-to-start deadline
	// during the window. Rate = deadline in sim ms.
	KindS2STimeout Kind = "s2s_timeout"
	// KindTransferDelay slows the history-transfer queue: each task
	// crossing the queue during the window is delayed once. Rate = delay
	// in sim ms.
	KindTransferDelay Kind = "transfer_delay"
	// KindPollerMin forces the active poller count down to Rate during the
	// window. Rate = forced poller count.
	KindPollerMin Kind = "poller_min"
	// KindTimerLoad turns on per-workflow timers during the window: each
	// workflow sleeps Rate ms before scheduling its activity. Rate = timer
	// duration in sim ms.
	KindTimerLoad Kind = "timer_load"
	// KindPersistenceStall multiplies the per-mutation persistence write
	// latency by Rate during the window. Rate = multiplier (0/1 = no
	// change).
	KindPersistenceStall Kind = "persistence_stall"
	// KindPollJitter makes poller long-polls expire early with probability
	// Rate (worker reconnect churn). Rate = probability.
	KindPollJitter Kind = "poll_jitter"
	// KindS2CShorten shrinks the activity ScheduleToClose deadline by Rate
	// ms during the window. Rate = deadline ms.
	KindS2CShorten Kind = "s2c_shorten"
	// KindBacklogSurge is a secondary arrival burst of Rate workflows at
	// StartMs. Rate = additional workflow count.
	KindBacklogSurge Kind = "backlog_surge"

	// RESERVED kinds (ADR-100): accepted by validation and returned by
	// State, but no leg behavior exists yet. A leg meeting one must fail
	// loudly ("unsupported on this leg"), never silently ignore it.
	KindPersistenceFail Kind = "persistence_fail"
	KindDuplicate       Kind = "duplicate"
	KindPartition       Kind = "partition"
)

// isKnownKind reports whether k is inside the closed kind set: the 12
// existing model kinds plus the 3 reserved kinds.
func isKnownKind(k Kind) bool {
	switch k {
	case KindAddDrop, KindWfDrop, KindPollDrop, KindActivityFailure,
		KindS2STimeout, KindTransferDelay, KindPollerMin, KindTimerLoad,
		KindPersistenceStall, KindPollJitter, KindS2CShorten, KindBacklogSurge,
		KindPersistenceFail, KindDuplicate, KindPartition:
		return true
	}
	return false
}

// isProbabilityKind reports whether k's Rate is a probability in [0, 1];
// every other kind's Rate is a non-negative scalar.
func isProbabilityKind(k Kind) bool {
	switch k {
	case KindAddDrop, KindWfDrop, KindPollDrop, KindPollJitter:
		return true
	}
	return false
}

// Window is one fault window of a schedule: the kind is active for the
// half-open interval [StartMs, EndMs) of elapsed sim ms. EndMs == 0 is
// open-ended from StartMs. Rate semantics are kind-specific (probability
// for the probability kinds, non-negative scalar for the rest).
type Window struct {
	Kind    Kind
	StartMs int64
	EndMs   int64
	Rate    float64
}

// ActiveFault is one kind currently active at a queried elapsed ms, with
// the rate its winning window carries.
type ActiveFault struct {
	Kind Kind
	Rate float64
}

// Source is the query seam both legs consume: a stateless, time-keyed
// decision function State(seed, elapsedMs) returning the faults active at
// elapsedMs under the schedule. *Schedule satisfies Source; a nil Source
// means "no schedule" at every call site.
type Source interface {
	State(seed int64, elapsedMs int64) []ActiveFault
}

// TracePoint is one recorded observation for the FromTrace replay seam: at
// elapsedMs the recorder observed exactly Faults active (the snapshot
// State returned at that instant). ElapsedMs must be strictly increasing
// across a trace.
type TracePoint struct {
	ElapsedMs int64
	Faults    []ActiveFault
}

// Schedule is an immutable, stateless fault schedule: a validated,
// deterministically sorted window list queried through the pure function
// State. A Schedule created from nil/empty windows is nil — the byte-neutral
// no-schedule state.
type Schedule struct {
	windows []Window // sorted by (StartMs, Kind); never mutated after NewSchedule
}

// NewSchedule validates windows and returns a schedule sorted by
// (StartMs, Kind). Validation rejects an unknown kind, a negative StartMs,
// EndMs < StartMs when EndMs != 0 (EndMs 0 is open-ended), a finite
// interval starting at math.MaxInt64 (no int64 EndMs could bound it), a
// non-finite Rate, a Rate outside [0, 1] for the probability kinds
// (add_drop/wf_drop/poll_drop/poll_jitter), and a negative Rate for the
// scalar kinds. nil or empty windows return (nil, nil) — the byte-neutral
// no-schedule state. seed is accepted so callers pass their canonical seed
// once at construction, but it is NOT stored: the schedule carries no
// mutable seed, and State takes the caller's seed per query (the seed is
// reserved for future stochastic kinds — WP0 windows are fully
// deterministic from their fields alone).
func NewSchedule(seed int64, windows []Window) (*Schedule, error) {
	if len(windows) == 0 {
		return nil, nil
	}
	for i, w := range windows {
		if !isKnownKind(w.Kind) {
			return nil, fmt.Errorf("faultsched: window %d: unknown kind %q", i, w.Kind)
		}
		if w.StartMs < 0 {
			return nil, fmt.Errorf("faultsched: window %d (%s): StartMs %d < 0", i, w.Kind, w.StartMs)
		}
		if w.EndMs != 0 && w.EndMs < w.StartMs {
			return nil, fmt.Errorf("faultsched: window %d (%s): EndMs %d < StartMs %d", i, w.Kind, w.EndMs, w.StartMs)
		}
		if w.EndMs != 0 && w.StartMs == math.MaxInt64 {
			return nil, fmt.Errorf("faultsched: window %d (%s): StartMs %d unreachable (EndMs %d cannot exceed it in an int64 interval)", i, w.Kind, w.StartMs, w.EndMs)
		}
		if err := validateRate(w.Kind, w.Rate); err != nil {
			return nil, fmt.Errorf("faultsched: window %d (%s, StartMs %d, EndMs %d): %w", i, w.Kind, w.StartMs, w.EndMs, err)
		}
	}
	sorted := append([]Window(nil), windows...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].StartMs != sorted[j].StartMs {
			return sorted[i].StartMs < sorted[j].StartMs
		}
		return sorted[i].Kind < sorted[j].Kind
	})
	return &Schedule{windows: sorted}, nil
}

// Windows returns the schedule's validated, deterministically sorted window
// list. The result shares the schedule's backing array — callers must treat
// it as read-only (the schedule is immutable after NewSchedule). A nil
// receiver returns nil. This is the enumeration seam legs use to validate
// kind coverage up front (ADR-100: a schedule naming a kind a leg cannot
// honor must fail loudly, never silently no-op).
func (s *Schedule) Windows() []Window {
	if s == nil {
		return nil
	}
	return s.windows
}

// validateRate checks the kind-specific Rate bounds: probability kinds
// require Rate in [0, 1], scalar kinds require Rate >= 0, and all kinds
// reject non-finite rates.
func validateRate(k Kind, rate float64) error {
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("rate %v is not finite", rate)
	}
	if isProbabilityKind(k) {
		if rate < 0 || rate > 1 {
			return fmt.Errorf("rate %v outside [0,1] for probability kind %s", rate, k)
		}
		return nil
	}
	if rate < 0 {
		return fmt.Errorf("rate %v < 0 for scalar kind %s", rate, k)
	}
	return nil
}

// State is the pure, time-keyed decision function: it returns the faults
// active at elapsedMs under the schedule, in sorted window order (a
// shadowing winner keeps its own sorted position). Overlap of the SAME
// kind resolves to the LAST active window by sorted order — the earlier
// window is shadowed and never appears; different kinds coexist. A nil
// receiver returns nil, and nil is returned whenever nothing is active.
// The result never aliases the schedule and is freshly allocated per call,
// so callers may mutate it freely.
//
// State is a pure function of (seed, elapsedMs): it reads no wall clock,
// spawns no goroutines, iterates no maps, and keeps no hidden state —
// repeated queries with the same arguments return identical, freshly
// allocated results. In WP0 the active set is fully determined by the
// windows and elapsedMs alone; the seed parameter is part of the Source
// seam (callers pass their canonical seed) and is reserved for future
// stochastic kinds — per-event coin flips stay leg-local on each side's
// own RNG (ADR-100).
func (s *Schedule) State(seed int64, elapsedMs int64) []ActiveFault {
	if s == nil || len(s.windows) == 0 {
		return nil
	}
	// Scan the sorted (by StartMs) windows, recording each kind's winning
	// active window (the LAST active one in sorted order). kinds and winIdx
	// stay parallel; winIdx holds the sorted index of each kind's winner.
	var kinds []Kind
	var winIdx []int
	for wi, w := range s.windows {
		if elapsedMs < w.StartMs {
			break // windows are sorted by StartMs: nothing later can be active
		}
		if w.EndMs != 0 && elapsedMs >= w.EndMs {
			continue // active only inside half-open [StartMs, EndMs)
		}
		if ki := indexOfKind(kinds, w.Kind); ki >= 0 {
			winIdx[ki] = wi // later active same-kind window shadows the earlier
		} else {
			kinds = append(kinds, w.Kind)
			winIdx = append(winIdx, wi)
		}
	}
	if len(kinds) == 0 {
		return nil
	}
	// Emit the winners in sorted window order (a kind's winner may sit at a
	// later position than its kind's first active window).
	order := make([]int, len(kinds))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return winIdx[order[a]] < winIdx[order[b]] })
	out := make([]ActiveFault, len(kinds))
	for i, oi := range order {
		out[i] = ActiveFault{Kind: kinds[oi], Rate: s.windows[winIdx[oi]].Rate}
	}
	return out
}

// indexOfKind returns the index of the first Kind in ks equal to k, or -1.
// The scanned slice is at most one entry per kind (never map-backed), so a
// linear scan is deterministic.
func indexOfKind(ks []Kind, k Kind) int {
	for i := range ks {
		if ks[i] == k {
			return i
		}
	}
	return -1
}

// FromTrace reconstructs a schedule from a recorded trace (the replay
// seam, ADR-100): each recorded fault becomes a 1 ms singleton window
// [ElapsedMs, ElapsedMs+1) carrying the recorded rate, so replay reuses
// the SAME State fast path — a trace point with several active kinds
// becomes one singleton per kind at that instant. Validation rejects
// non-monotonic elapsedMs (strictly increasing required, matching the
// exclusive half-open end) and negative ElapsedMs. nil or empty traces
// return (nil, nil) — the byte-neutral no-schedule state.
func FromTrace(seed int64, trace []TracePoint) (*Schedule, error) {
	if len(trace) == 0 {
		return nil, nil
	}
	var windows []Window
	for i, tp := range trace {
		if tp.ElapsedMs < 0 {
			return nil, fmt.Errorf("faultsched: trace point %d: ElapsedMs %d < 0", i, tp.ElapsedMs)
		}
		if tp.ElapsedMs == math.MaxInt64 {
			return nil, fmt.Errorf("faultsched: trace point %d: ElapsedMs %d too large (1 ms singleton window [ElapsedMs, ElapsedMs+1) would overflow)", i, tp.ElapsedMs)
		}
		if i > 0 && tp.ElapsedMs <= trace[i-1].ElapsedMs {
			return nil, fmt.Errorf("faultsched: trace point %d: ElapsedMs %d breaks monotonicity (previous %d; must be strictly increasing)",
				i, tp.ElapsedMs, trace[i-1].ElapsedMs)
		}
		for _, f := range tp.Faults {
			windows = append(windows, Window{
				Kind:    f.Kind,
				StartMs: tp.ElapsedMs,
				EndMs:   tp.ElapsedMs + 1, // exclusive end of the 1 ms singleton (overflow rejected above)
				Rate:    f.Rate,
			})
		}
	}
	return NewSchedule(seed, windows)
}

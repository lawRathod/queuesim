package model

import (
	"fmt"

	"github.com/lawRathod/queuesim/godes/observe"
)

// VectorKind identifies a decision-vector fault kind (ADR-069). Each kind
// maps 1:1 to retry_amp config fields via the Confirm overrides map.
type VectorKind string

const (
	// KindAddDrop drops a fraction of tasks entering the matching queue
	// (transport-level AddTask drop). Rate = probability.
	KindAddDrop VectorKind = "add_drop"
	// KindPollDrop fails a fraction of poller polls (no dispatch that
	// poll). Rate = probability.
	KindPollDrop VectorKind = "poll_drop"
	// KindTransferDelay slows the history-transfer queue: each task
	// crossing the queue during the window is delayed once. Rate = delay
	// in sim ms.
	KindTransferDelay VectorKind = "transfer_delay"
	// KindActivityFailure makes activity attempts fail during the window
	// (overrides the default fault window). Rate unused.
	KindActivityFailure VectorKind = "activity_failure"
	// KindWfDrop drops a fraction of wf-class tasks at the matching-wf
	// queue during the window (plan 2026-08-12 D3): wf-class starvation —
	// dropped wf tasks re-enter via the transfer queue, so attempts rise
	// and completions delay. Rate = probability.
	KindWfDrop VectorKind = "wf_drop"
	// KindS2STimeout sets the matching queues' MaxWaitMs (schedule-to-start
	// deadline) during the window (plan 2026-08-12 D2): tasks whose
	// queue residency exceeds the deadline expire — terminal, non-retryable
	// workflow failures. Rate = deadline in sim ms.
	KindS2STimeout VectorKind = "s2s_timeout"
	// KindTimerLoad turns on per-workflow timers during the window
	// (plan 2026-08-12 D2/C5): each workflow sleeps Rate ms before
	// scheduling its activity — sustained wf-task re-entry load. Rate =
	// timer duration in sim ms.
	KindTimerLoad VectorKind = "timer_load"
	// KindPollerMin forces the active poller count down to Rate during the
	// window (plan 2026-08-12 D4): poller starvation → backlog growth.
	// Rate = forced poller count.
	KindPollerMin VectorKind = "poller_min"

	// New v2 fault kinds (2026-08-21 fault expansion, ADR-078 follow-up).

	// KindPersistenceStall multiplies the per-mutation persistence write
	// latency by Rate during the window (history backpressure). Rate =
	// multiplier (1 = no change, 2 = double latency, ...); 0 = no change.
	KindPersistenceStall VectorKind = "persistence_stall"
	// KindPollJitter makes poller long-polls expire early with probability
	// Rate (worker reconnect churn — pollers re-poll frequently). Rate =
	// early-expiry probability per long-poll.
	KindPollJitter VectorKind = "poll_jitter"
	// KindS2CShorten shrinks the activity ScheduleToClose deadline by Rate
	// ms during the window (a tight deadline forces timeouts mid-retry).
	// Rate = deadline ms.
	KindS2CShorten VectorKind = "s2c_shorten"
	// KindBacklogSurge is a secondary arrival burst of Rate workflows at
	// StartMs (frontend load spike on top of the primary burst). Rate =
	// additional workflow count.
	KindBacklogSurge VectorKind = "backlog_surge"
)

// IsV2Fault reports whether the kind is a v2-only fault (added in the
// 2026-08-21 expansion; no v1 legacy equivalent).
func (k VectorKind) IsV2Fault() bool {
	switch k {
	case KindPersistenceStall, KindPollJitter, KindS2CShorten, KindBacklogSurge:
		return true
	}
	return false
}

// DecisionVector is the 5-tuple fault specification shared by the search
// harness and the confirm leg (ADR-069): {kind, target, start, duration,
// rate} plus a Confirm overrides map that merges into the retry_amp
// config.json on the bubble side (no retry_amp import — the merge is pure
// JSON map surgery). A composed vector (Compose != nil) is the concurrent
// application of up to two distinct-kind members (plan 2026-08-12
// sustained-composed-search): its own Kind/Target/Start/Duration/Rate must
// be zero and the members carry the fault tuples.
type DecisionVector struct {
	Name       string           `json:"name"`
	Kind       VectorKind       `json:"kind"`
	Target     string           `json:"target"` // "matching" | "transfer" | "activity"; "" = kind default
	StartMs    int64            `json:"start"`
	DurationMs int64            `json:"duration"` // 0 = open-ended
	Rate       float64          `json:"rate"`     // kind-specific semantics (see kinds)
	Confirm    map[string]any   `json:"confirm,omitempty"`
	Compose    []DecisionVector `json:"compose,omitempty"`
	// FaultWindow is grid-control, not a fault-tuple element: "off" forces
	// the default activity-failure window closed for the vector, "on"
	// forces it open, "" = the kind-based harness default. Used by grid
	// authors to build policy-matched solo baselines for composed vectors.
	FaultWindow string `json:"fault_window,omitempty"`
}

// Members returns the member set this vector applies: the vector itself for
// a plain vector, the declared Compose slice for a composed one. The
// returned slice is a copy — callers may not mutate the vector through it.
// Determinism: slice order (never a map).
func (v DecisionVector) Members() []DecisionVector {
	if len(v.Compose) == 0 {
		return []DecisionVector{v}
	}
	out := make([]DecisionVector, len(v.Compose))
	copy(out, v.Compose)
	return out
}

// ActiveAt reports whether the vector's window covers nowMs: half-open
// [StartMs, StartMs+DurationMs); DurationMs 0 = open-ended from StartMs.
func (v DecisionVector) ActiveAt(nowMs int64) bool {
	if nowMs < v.StartMs {
		return false
	}
	if v.DurationMs <= 0 {
		return true
	}
	return nowMs < v.StartMs+v.DurationMs
}

// VectorAxis is one grid axis for ExpandGrid: a kind/target/window start
// combined over durations × rates — or, with Compose set, one composed
// vector over the member axes (each member must expand to a singleton; the
// grid validator enforces this). Confirm is the grid-level bubble override
// merged LAST over the members' own confirms (e.g. harness.pollers for the
// composed poller_min confirm leg). FaultWindow is the grid-level
// default-window policy override (see DecisionVector.FaultWindow).
type VectorAxis struct {
	Name        string         `json:"name,omitempty"` // explicit vector name (calm axes; debug); "" = generated
	Kind        VectorKind     `json:"kind"`
	Target      string         `json:"target"`
	StartMs     int64          `json:"start"`
	Durations   []int64        `json:"durations"`
	Rates       []float64      `json:"rates"`
	Compose     []VectorAxis   `json:"compose,omitempty"`
	Confirm     map[string]any `json:"confirm,omitempty"`
	FaultWindow string         `json:"fault_window,omitempty"`
}

// ExpandGrid expands axes into the full combinatorial vector list in
// deterministic order: axes in input order, durations outer, rates inner.
// Names encode the tuple: <kind>-<target>-<start>-<duration>-<rate>. A
// composed axis expands to one vector per member-vector combination (the
// cartesian product over member expansions, member order preserved), named
// "compose-<m1>-<m2>-..."; axis Confirm and FaultWindow propagate to every
// expanded vector.
func ExpandGrid(axes []VectorAxis) []DecisionVector {
	var out []DecisionVector
	for _, ax := range axes {
		if len(ax.Compose) > 0 {
			for _, combo := range expandComposed(ax) {
				out = append(out, combo)
			}
			continue
		}
		for _, v := range expandAxis(ax) {
			out = append(out, v)
		}
	}
	return out
}

// expandAxis expands one plain axis into its singleton vectors.
func expandAxis(ax VectorAxis) []DecisionVector {
	var out []DecisionVector
	for _, d := range ax.Durations {
		for _, r := range ax.Rates {
			name := ax.Name
			if name == "" {
				name = fmt.Sprintf("%s-%s-%d-%d-%v", ax.Kind, ax.Target, ax.StartMs, d, r)
			}
			out = append(out, DecisionVector{
				Name:        name,
				Kind:        ax.Kind,
				Target:      ax.Target,
				StartMs:     ax.StartMs,
				DurationMs:  d,
				Rate:        r,
				Confirm:     ax.Confirm,
				FaultWindow: ax.FaultWindow,
			})
		}
	}
	return out
}

// expandComposed expands a composed axis: the cartesian product of the
// member expansions in member order (deterministic nested loops). The
// result carries no kind — the members are the fault tuples.
func expandComposed(ax VectorAxis) []DecisionVector {
	var combos [][]DecisionVector
	combos = append(combos, []DecisionVector{})
	for _, m := range ax.Compose {
		next := make([][]DecisionVector, 0, len(combos)*len(m.Durations)*len(m.Rates))
		for _, prefix := range combos {
			for _, mv := range expandAxis(m) {
				next = append(next, append(append([]DecisionVector{}, prefix...), mv))
			}
		}
		combos = next
	}
	var out []DecisionVector
	for _, combo := range combos {
		name := "compose"
		for _, m := range combo {
			name += "-" + m.Name
		}
		out = append(out, DecisionVector{
			Name:        name,
			Compose:     combo,
			Confirm:     ax.Confirm,
			FaultWindow: ax.FaultWindow,
		})
	}
	return out
}

// MergeOverlays returns a deep copy of base with overlays merged over it in
// order — for each key the LAST overlay's value wins, recursively for maps.
// A nil base is treated as empty. The result never aliases base or any
// overlay (pure function of its inputs — deterministic despite map
// iteration, because each overlay's keys are unique within it and overlays
// apply in slice order).
func MergeOverlays(base map[string]any, overlays ...map[string]any) map[string]any {
	merged := deepCopyMap(base)
	for _, o := range overlays {
		if o == nil {
			continue
		}
		mergeMaps(merged, o)
	}
	return merged
}

// ToConfirmJSON returns a deep copy of base (a config.json object map) with
// v.Confirm recursively merged over it. Scalar and map values from Confirm
// override the base; unrelated sections pass through untouched. A nil
// Confirm returns a copy of base. The merge is a pure function of its
// inputs — deterministic and free of any retry_amp dependency.
func (v DecisionVector) ToConfirmJSON(base map[string]any) (map[string]any, error) {
	if base == nil {
		return nil, fmt.Errorf("model: ToConfirmJSON requires a non-nil base config map")
	}
	merged := deepCopyMap(base)
	mergeMaps(merged, v.Confirm)
	return merged, nil
}

// deepCopyMap copies a JSON object map recursively (arrays by shallow
// copy — Confirm never targets arrays).
func deepCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			out[k] = deepCopyMap(sub)
		} else {
			out[k] = v
		}
	}
	return out
}

// mergeMaps overlays src onto dst recursively: maps merge key-wise; any
// non-map value in src replaces the dst value. Map values taken from src
// are deep-copied so the overlay is never aliased (MergeOverlays is a pure
// function of its inputs).
func mergeMaps(dst, src map[string]any) {
	for k, sv := range src {
		if sm, ok := sv.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				mergeMaps(dm, sm)
				continue
			}
			dst[k] = deepCopyMap(sm)
			continue
		}
		dst[k] = sv
	}
}

// Regime is the metastability classification primitive (ADR-069):
// metastable (declared, never recovered), recoverable (declared then
// recovered), nothing. Retained call-site-free after the 2026-08-13
// reporting-layer removal (ADR-071).
type Regime string

const (
	RegimeNothing     Regime = "nothing"
	RegimeMetastable  Regime = "metastable"
	RegimeRecoverable Regime = "recoverable"
)

// RegimeFromStatus classifies a monitor status snapshot.
func RegimeFromStatus(s observe.MonitorStatus) Regime {
	if !s.Metastable {
		return RegimeNothing
	}
	if s.Recovered {
		return RegimeRecoverable
	}
	return RegimeMetastable
}

// MonitorConfig returns the monitor configuration shared by the search
// harness and the bubble confirm leg (ADR-069). It is byte-for-byte the
// retry_amp monitor literal (WindowMs 100, BaselineWarmupWindows 50,
// MetastablePersistWindows 50); the fields that literal leaves zero
// (RecoveryHoldWindows, BaselineEpsilon) stay zero so both legs observe
// IDENTICAL semantics by construction.
//
// Known consequence: the BaselineEpsilon 0 / RecoveryHoldWindows 0
// degeneracy (unobservable metastable class) is analyzed in ADR-071 and
// /.agents/research/2026-08-13-regime-taxonomy-removal.md.
func MonitorConfig() observe.MonitorConfig {
	return observe.MonitorConfig{
		WindowMs:                 100,
		BaselineWarmupWindows:    50,
		MetastablePersistWindows: 50,
	}
}

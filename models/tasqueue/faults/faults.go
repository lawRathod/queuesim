// Package faults implements the tasqueue model's windowed fault injection
// as a self-contained subpackage: each Fault declares a time window
// [StartMs, EndMs) during which it overrides one of the static mechanism
// knobs (DropRate / FailP / TimeoutMs) at its real chokepoint, restoring
// the config baseline after the window closes. The application logic for
// each kind lives in its own file here, operating ONLY through the Target
// interface — this package never touches the model's unexported internals.
//
// File map:
//
//	drop.go     KindDrop     queue drain-time delivery drop (attempt-level failure)
//	failure.go  KindFailure  handler failure probability
//	timeout.go  KindTimeout  attempt timeout deadline (ServiceMs > timeout)
//
// Apply is the pump-time dispatcher: for each kind the LAST active fault
// wins (mirrors model/faults' Injector last-active-wins and the temporal
// scalar "last wins"), then the per-kind application functions in the
// sibling files restore-or-override the target knob.
//
// Determinism: with an empty fault list Apply early-returns — zero
// additional work and the run is byte-identical to a config with no
// faults. With a configured-but-inactive list it does only O(1) idempotent
// knob resets (SetDropRate of the baseline) and makes zero RNG draws —
// also byte-identical to the no-fault path (pinned by
// TestFaultsInactiveZeroDraw). When a stochastic fault is active it draws
// only through the queue's existing drop substream (created lazily on
// first rate > 0) and/or the fail substream (created lazily on the first
// failing attempt); the timeout fault consumes no RNG at all. All windows
// are evaluated on the sim clock — no wall-clock, no goroutines, no map
// iteration. Window state is sampled once per pump tick (the model's 1ms
// cadence), so a take/completion at a window edge observes the previous
// tick's state — deterministic and digest-pinned.
package faults

// FaultKind identifies a windowed fault kind.
type FaultKind string

const (
	// KindDrop drops a fraction of queue deliveries during the window. A
	// drop is modeled as an attempt-level failure consuming the retry
	// budget (config.go DropRate) — the contract with
	// TestDropConsumesRetryBudget.
	KindDrop FaultKind = "tasqueue_drop"
	// KindFailure raises the handler failure probability during the window
	// (config.go FailP semantics).
	KindFailure FaultKind = "tasqueue_failure"
	// KindTimeout makes attempts time out during the window: an attempt
	// times out iff ServiceMs > the window's Rate deadline (config.go
	// TimeoutMs / server.go:388-390 deadline semantics).
	KindTimeout FaultKind = "tasqueue_timeout"
)

// Fault is one declarative, time-windowed fault injection. Rate is
// kind-specific: drop probability [0,1], failure probability [0,1], or
// timeout deadline in sim ms.
type Fault struct {
	// Kind is the fault kind.
	Kind FaultKind
	// StartMs is the window start in sim ms (inclusive).
	StartMs int64
	// EndMs is the window end in sim ms (exclusive); EndMs must be >=
	// StartMs.
	EndMs int64
	// Rate is kind-specific (see Fault).
	Rate float64
}

// activeAt reports whether the fault's [StartMs, EndMs) window covers
// nowMs.
func (f Fault) activeAt(nowMs int64) bool {
	return nowMs >= f.StartMs && nowMs < f.EndMs
}

// Apply applies the windowed fault knobs at the pump. This function is the
// switch + per-kind accumulation only — each kind's application lives in
// its own file (file map above). The tasqueue model's runtime calls this
// once per pump tick with itself as the Target and its Config.Faults
// (fault_target.go). An empty fault list early-returns: zero work, zero
// RNG draws (byte-identical to the no-fault path).
func Apply(t Target, faultList []Fault, nowMs int64) {
	if len(faultList) == 0 {
		return
	}
	// For each kind, the LAST active fault wins (mirrors model/faults'
	// Injector last-active-wins and the temporal scalar "last wins").
	dropRate, hasDrop := activeFaultRate(faultList, KindDrop, nowMs)
	failP, hasFail := activeFaultRate(faultList, KindFailure, nowMs)
	timeoutMs, hasTimeout := activeFaultRate(faultList, KindTimeout, nowMs)
	applyDropFault(t, dropRate, hasDrop)
	applyFailureFault(t, failP, hasFail)
	applyTimeoutFault(t, timeoutMs, hasTimeout)
}

// activeFaultRate returns the rate (and presence) of the LAST active fault
// of the given kind at nowMs. Iterating the slice in order makes "last
// active wins" deterministic — no map iteration.
func activeFaultRate(faults []Fault, kind FaultKind, nowMs int64) (rate float64, found bool) {
	for _, f := range faults {
		if f.Kind == kind && f.activeAt(nowMs) {
			rate = f.Rate
			found = true
		}
	}
	return rate, found
}

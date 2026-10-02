package faults

// Target abstracts every chokepoint the windowed faults touch on the
// tasqueue model's runtime: the broker queue's drain-time drop rate, the
// runtime's effective failure-probability / timeout-deadline knobs, and the
// static config baselines the appliers restore when no window is active.
// The fault files apply ONLY through this interface — the model implements
// it with small adapter methods that forward to the queue and the runtime
// fields (fault_target.go), so this package never reaches into the model's
// internals.
type Target interface {
	// SetDropRate sets the broker queue's drain-time delivery drop
	// probability (0 = off). A rate > 0 creates the queue's drop substream
	// lazily on first use; a rate of 0 disables drops with zero RNG draws.
	SetDropRate(rate float64)
	// SetEffectiveFailP sets the effective per-attempt handler failure
	// probability, consulted by complete for the RNG decision.
	SetEffectiveFailP(p float64)
	// SetEffectiveTimeoutMs sets the effective attempt timeout deadline in
	// sim ms (0 = none), consulted by take for the ServiceMs > deadline
	// check.
	SetEffectiveTimeoutMs(ms int64)
	// DropBaseline returns the static DropRate restored when no KindDrop
	// window is active.
	DropBaseline() float64
	// FailPBaseline returns the static FailP restored when no KindFailure
	// window is active.
	FailPBaseline() float64
	// TimeoutBaseline returns the static TimeoutMs restored when no
	// KindTimeout window is active.
	TimeoutBaseline() int64
}

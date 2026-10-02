package faults

// applyFailureFault raises the per-attempt handler failure probability
// (KindFailure) at its real chokepoint: the effective FailP consulted by
// complete for the RNG decision. While a KindFailure window is active, the
// effective failure probability is the fault's rate; otherwise it is the
// config baseline FailP. The decision is read at COMPLETION time (when the
// handler's success/failure materializes), so an attempt whose completion
// falls in the window is judged at the fault rate and one taken just before
// the window but completing inside is judged inside it — deterministic:
// the judgment is sampled at the completion tick (the pump's 1ms
// granularity); the ±ServiceMs spread is the take→completion time.
// Consumes RNG only when the effective rate is > 0 (the fail substream is
// created lazily on the first failing attempt — runtime.failDraw), so a
// 0/off window draws nothing.
func applyFailureFault(t Target, rate float64, active bool) {
	p := t.FailPBaseline()
	if active {
		p = rate
	}
	t.SetEffectiveFailP(p)
}

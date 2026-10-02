package faults

// applyDispatchRate applies the dispatch-rate collapse (KindPollDrop): for
// the window, the token-bucket dispatch rate on both matching queues drops
// to DispatchRate·(1−pollDrop) — a sustained arrival burst against a
// collapsed dispatch (the rate recovers when the member goes inactive, via
// the else-branchless reconfiguration each tick). pollDrop < 0 (no active
// member) leaves the queues' constructed rate untouched; a non-positive
// base rate means dispatch is already unlimited. Fault-control (taxonomy
// in models/temporal/doc.go): a test knob at the token-bucket dispatch
// chokepoint, not a server pin.
func applyDispatchRate(t Target, spec Spec, pollDrop float64) {
	if pollDrop < 0 {
		return
	}
	base := spec.DispatchRate
	if base <= 0 {
		return
	}
	eff := base * (1 - pollDrop)
	t.SetDispatchRate(eff, float64(spec.DispatchBurst))
}

package faults

// applyS2STimeout applies the activity schedule-to-start deadline
// (KindS2STimeout): MaxWaitMs on the matching-act queue. The deadline is
// exclusive — a task born at t expires at poll once now−BornMs > MaxWaitMs
// (deadline+1 semantics: it survives exactly MaxWaitMs ms, then expires on
// the next ms) and expiry is terminal (dispatchFrom counts it Failed).
// With no active member (s2sTimeout < 0), the config ActivityS2STimeoutMs —
// the queue's constructed value — applies. Fault-control (taxonomy in
// models/temporal/doc.go): a test knob at the matching-act MaxWaitMs
// chokepoint, not a server pin.
func applyS2STimeout(t Target, spec Spec, s2sTimeout int64) {
	if s2sTimeout >= 0 {
		t.SetMaxWaitAct(s2sTimeout)
	} else {
		t.SetMaxWaitAct(spec.ActivityS2STimeoutMs)
	}
}

package faults

// wfSpoolRate computes the matching-wf spool-drop rate for wf_drop
// (KindWfDrop, class starvation): the max of the add_drop transport-loss
// rate and the wf_drop rate, clamped to [0, ∞). The queue application is
// shared with add_drop — applySpoolDrop (add_drop.go) sets the wf queue
// from this rate and the act queue from add_drop alone. Fault-control
// (taxonomy in models/temporal/doc.go): a test knob at the matching-wf
// spool-drop chokepoint, not a server pin.
func wfSpoolRate(addDrop, wfDrop float64) float64 {
	rate := addDrop
	if wfDrop > rate {
		rate = wfDrop
	}
	if rate < 0 {
		rate = 0
	}
	return rate
}

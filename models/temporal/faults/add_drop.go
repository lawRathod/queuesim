package faults

// applySpoolDrop applies spool-time drop rates to the matching queues.
// add_drop (KindAddDrop, transport loss) lands on BOTH matching queues; the
// matching-wf rate additionally folds in the wf_drop class-starvation rate
// via wfSpoolRate (wf_drop.go). Rates below zero (no active member) clamp
// to 0 — the queues' default. Fault-control (taxonomy in
// models/temporal/doc.go): a test knob at the spool-time drop chokepoint,
// not a server pin.
func applySpoolDrop(t Target, addDrop, wfDrop float64) {
	actRate := addDrop
	if actRate < 0 {
		actRate = 0
	}
	t.SetSpoolDrop(wfSpoolRate(addDrop, wfDrop), actRate)
}

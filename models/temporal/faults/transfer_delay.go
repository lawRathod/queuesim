package faults

// applyTransferDelay applies the dispatch delay (KindTransferDelay) on both
// matching queues: every dispatched task is handed to its worker
// dispatchDelay ms after the queue pops it (latency chokepoint). With no
// active member (transferDelay < 0), the delay resets to 0 — the queues'
// default. Fault-control (taxonomy in models/temporal/doc.go): a test
// knob at the matching dispatch-delay chokepoint, not a server pin.
func applyTransferDelay(t Target, transferDelay int64) {
	if transferDelay >= 0 {
		t.SetDispatchDelay(transferDelay)
	} else {
		t.SetDispatchDelay(0)
	}
}

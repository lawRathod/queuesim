package faults

// applyPollerMin applies the forced low poller count (KindPollerMin, worker
// starvation): while active, maintainWorkers and the repoll hooks target
// this count for BOTH classes instead of the configured fleet (matching.go),
// starving the queues of parked pollers. -1 = no active member → the
// configured fleet (and any elastic scale-up) applies. Fault-control
// (taxonomy in models/temporal/doc.go): a test knob at the poller-count
// chokepoint, not a server pin.
func applyPollerMin(t Target, pollerForced int) {
	t.SetPollerForced(pollerForced)
}

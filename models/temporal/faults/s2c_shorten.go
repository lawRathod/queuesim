package faults

// applyS2CShorten applies the ScheduleToClose override (KindS2CShorten):
// while active, the activity's schedule-to-close deadline uses the member's
// rate instead of the config value (complete, history.go — the deadline
// counts from the scheduled-at time, so shortening it converts slow
// executions into failures). 0 = no active override → the config
// ScheduleToCloseMs applies. Fault-control (taxonomy in
// models/temporal/doc.go): a test knob at the schedule-to-close deadline
// chokepoint, not a server pin.
func applyS2CShorten(t Target, rate float64) {
	t.SetS2cMs(int64(rate))
}

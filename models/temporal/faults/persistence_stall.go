package faults

// applyPersistenceStall applies the persistence latency multiplier
// (KindPersistenceStall): while active, every history-shard persist scales
// its landing time by the member's rate (PersistScaled — the persistence
// write path slows by rate×, so tasks land later and the shard's cursors
// lag). Only positive rates take effect; 1 = no change (the default).
// Fault-control (taxonomy in models/temporal/doc.go): a test knob at the
// persistence-latency chokepoint, not a server pin.
func applyPersistenceStall(t Target, rate float64) {
	if rate > 0 {
		t.SetPersistMult(rate)
	}
}

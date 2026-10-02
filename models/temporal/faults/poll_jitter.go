package faults

// applyPollJitter applies the poller long-poll early-expiry probability
// (KindPollJitter): while active, each newly parked poller expires early
// (LongPollMs/10) with probability rate — worker reconnect churn that
// interrupts long polls (parkWithJitter, matching.go). Only non-negative
// rates take effect; 0 = off (zero RNG draws; the jitter substream is
// created lazily on the first draw). Fault-control (taxonomy in
// models/temporal/doc.go): a test knob at the long-poll expiry
// chokepoint, not a server pin.
func applyPollJitter(t Target, rate float64) {
	if rate >= 0 {
		t.SetPollJitterP(rate)
	}
}

package faults

// applyTimeoutFault makes attempts time out (KindTimeout) at its real
// chokepoint: the effective TimeoutMs deadline evaluated in take. While a
// KindTimeout window is active, the effective timeout is the fault's rate
// (in sim ms), so an attempt times out iff ServiceMs > rate
// (server.go:388-390 deadline semantics); otherwise it is the config
// baseline TimeoutMs. Rate is truncated to whole sim ms while the window is
// active (int64(rate) recomputed each tick); a fractional deadline floors,
// which is irrelevant for the strict ServiceMs > timeout decision in
// practice. Deadline evaluation is pure sim-time math — zero RNG draws.
func applyTimeoutFault(t Target, rate float64, active bool) {
	ms := t.TimeoutBaseline()
	if active {
		ms = int64(rate)
	}
	t.SetEffectiveTimeoutMs(ms)
}

package faults

// applyDropFault applies the drain-time delivery drop rate (KindDrop) at
// its real chokepoint: the pending queue's SetDropRate. While a KindDrop
// window is active, the queue drops with the fault's rate; otherwise it
// runs at the config baseline DropRate (0 = off). A drop is an
// attempt-level failure that consumes the retry budget and never reaches
// the handler (config.go DropRate) — the contract pinned by
// TestDropConsumesRetryBudget. Setting the rate to 0 disables drops with
// zero RNG draws; a rate > 0 creates the queue's drop substream lazily on
// first use.
func applyDropFault(t Target, rate float64, active bool) {
	dropRate := t.DropBaseline()
	if active {
		dropRate = rate
	}
	t.SetDropRate(dropRate)
}

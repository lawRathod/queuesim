package temporal

import "github.com/lawRathod/queuesim/model/monitor"

// HistoryOccupancyEvidence is the summary-time history-queue occupancy: the
// fraction of pump ticks on which each history queue surface was NON-EMPTY,
// folded from the EXISTING unlabeled gauge series (Monitor.GaugeSeries). This
// is the honest occupancy measure for history — the shard's worker counts
// have NO per-tick effect (components/history/shard.go), so a busy-worker
// utilization gauge would export a mechanism the model does not simulate.
// Zero-value when monitoring is disabled or nothing was sampled.
type HistoryOccupancyEvidence struct {
	// Transfer is the transfer-queue non-empty fraction (0..1).
	Transfer float64
	// Timer is the timer-queue non-empty fraction (0..1).
	Timer float64
	// Resched is the reschedule-queue non-empty fraction (0..1).
	Resched float64
}

// HistoryOccupancy computes the queue-non-empty fraction of each history
// surface at summary time (model/monitor NonEmptyFraction folded over the
// existing gauge series). Pure read; requires Run to have finalized. A nil
// receiver, disabled monitor, or unrun topology returns the zero value.
func (t *Topology) HistoryOccupancy() HistoryOccupancyEvidence {
	if t == nil || t.rt == nil || t.sys == nil {
		return HistoryOccupancyEvidence{}
	}
	m := t.sys.Monitor()
	if m == nil {
		return HistoryOccupancyEvidence{}
	}
	return HistoryOccupancyEvidence{
		Transfer: monitor.NonEmptyFraction(m.GaugeSeries("history.shards.transfer")),
		Timer:    monitor.NonEmptyFraction(m.GaugeSeries("history.shards.timer")),
		Resched:  monitor.NonEmptyFraction(m.GaugeSeries("history.shards.resched")),
	}
}

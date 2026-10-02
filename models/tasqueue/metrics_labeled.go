package tasqueue

import "github.com/lawRathod/queuesim/model/monitor"

// attachLabeledMetrics registers the OPT-IN labeled/attributed metric
// surfaces (Config.LabeledMetrics) for the one tasqueue queue
// ("tasqueue:tasks"). It is the occupancy leg of the load definition
// (model/monitor load vocabulary), decomposed into the two surfaces the
// unlabeled tasqueue.queue.depth gauge SUMS: the broker queue and the
// dispatch buffer (tasks drained from the broker awaiting a poller take).
//
// COST (ADR-115): 2 labeled gauges, each retaining a full per-tick series
// (model/monitor doc.go) — ~2×(SimEndMs/PumpMs) retained samples.
//
// OFF by default (Config.LabeledMetrics false): nothing here runs and the
// determinism digest is unmoved. The closures are pure reads — zero RNG
// draws, zero simulation mutation.
func (rt *runtime) attachLabeledMetrics(m *monitor.Monitor) {
	depth := monitor.LoadName("tasqueue", monitor.LoadQueueDepth)
	broker := []monitor.Label{{Key: "queue", Value: "tasks"}, {Key: "surface", Value: "broker"}}
	dispatch := []monitor.Label{{Key: "queue", Value: "tasks"}, {Key: "surface", Value: "dispatch"}}
	m.AddGaugeKey(depth, broker, func() int64 { return int64(rt.q.Len()) })
	m.AddGaugeKey(depth, dispatch, func() int64 { return int64(len(rt.dispatch)) })
}

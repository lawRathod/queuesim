// Package temporal models Temporal's task-queue machinery (matching,
// history) as Services built on the GoDES modeling library (ADR-093). The
// model is mechanism-faithful to the real Temporal server: every modeled
// mechanism carries one of four traceability labels — Pinned (a direct
// `temporal/` submodule file:line source; component ↔ file:line map in
// contract_test.go), Calibrated (a bubble-fitted value with its stated
// residual, e.g. ChildDrainFraction 0.75), Structural-neutral (a neutral
// default chosen for structural honesty, neither pinned nor fitted), or
// Fault-control (a decision-vector test knob applied at a real chokepoint
// in faults/, not a server constant) — and correctness is measured as
// mechanism fidelity, not bubble-gap calibration (parity-era gates retired
// 2026-08-26).
//
// # Services
//
// The package defines two model.Service values — matching, history — each
// materializing its real primitives (MatchQueues, poller fleets, history
// shards, sticky queue) directly on the shared runtime's simulation, wired
// into a model.System by BuildSystem. There is deliberately NO frontend
// service: the StartWorkflow RPC adds no mechanical chip in this model (its
// latency is folded into the startWorkflow atMs as NetworkMs), so the
// workload source (burst/arrival/surge, load.go) is a runtime scheduling
// behavior, not a Service. The services deliberately share one Go package
// and one runtime mediator: the real Temporal services share internals too
// (ADR-093 pragmatism), and the matching↔history runtime data loop
// (matching reject/drop → history transfer/reschedule → matching re-add) is
// driven by that mediator. The System owns the core.Simulation and the free
// monitor; models/temporal owns only model logic.
//
// # Schematic honesty
//
// The model declares NO builder ports and wires NO sys.Connect edges: the
// real data loop above is runtime-mediated, so a port shell / zero-cost
// Connect edge would render in the DOT service graph without a single task
// traversing it ("if the code doesn't traverse it, the diagram doesn't draw
// it"). Generated DOT is therefore a STRUCTURAL-DEBUG artifact — it renders
// REGISTERED builder primitives only, and this model registers none, so its
// service clusters render empty (pinned by dot_test.go). The architecture
// figure is DERIVED FROM OBSERVED BEHAVIOR instead: set Config.Capture and
// the runtime records every mechanism transition at its real chokepoints
// into a model/capture Recorder (Topology.Capture); capture.Aggregate +
// Graph.WriteDOT render the mechanism map from what actually traversed the
// model — never hand-drawn from a component table. The recorded component
// names are this model's own (load/history/matching/exec/terminal), keyed
// to the mechanism surface below and the ADR-078 component ↔ file:line map
// for the real-server pin.
//
// # Mechanism surface
//
//   - load (runtime): burst/arrival/surge inflow; workflow-start
//     (startWorkflow → history persist → transfer → matching-wf)
//   - matching: matching-wf/act queues with sync-match
//     (MatchingSyncMatchWaitDuration 200ms), long-poll parking, elastic
//     poller scaling, token-bucket dispatch, spool/drop/reject
//   - history: range-owned shards, sticky queues, transfer/timer
//     processors, rescheduler (1000ms / 1.1× / 3min cap), retry/timers,
//     lifecycle executor (runWF/runAct/complete)
//
// # Monitoring (free, zero config)
//
// Because the model is built on the library, every run automatically
// carries monitored results (metrics.go): a schedule-to-start latency
// histogram per activity execution (MetricS2SLatency, percentile summary
// p50/p90/p99/max), throughput/outcome counters (MetricStarted /
// MetricActivityAdds / MetricCompleted / MetricRetries / MetricFailed),
// and queue-depth gauges (matching-wf/act/sticky/pollers/in-flight,
// history transfer/timer/resched). They flow into Result.Metrics with
// zero model configuration; the WP3 gates pin determinism (N=5 bit-exact),
// monitor on/off neutrality, and the structural-debug DOT honesty gates
// (TestTemporalDotStructuralDebug).
//
// # Determinism
//
// Single-goroutine, pump-driven (model.System owns the pump); no
// wall-clock; no map iteration on simulation paths; RNG only via
// core.RNG substreams (path-prefixed, constructor-fixed); monitor-neutral
// (the free monitor makes zero RNG draws and samples in sorted order —
// on/off runs are bit-identical). Same-time events tie-break by
// core.FIFOPolicy (ComponentID, TaskID, Seq) — deterministic because the
// model schedules in a fixed order on the single owned Simulation.
// Enforced by determinism_test.go (N=5 bit-exact + monitor on/off, with
// and without stochastic substreams) and by review.
package temporal

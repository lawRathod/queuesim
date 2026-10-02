package temporal

import (
	"fmt"
	"io"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
	"github.com/law/GoDES/observe"
	"github.com/law/GoDES/seed"
)

// Topology is the thin execution handle over a built model.System: it owns
// the System (which OWNS the core.Simulation) plus the shared runtime, and
// exposes the run-level Result surface (Stats/Status/DrainMs + free-monitor
// metrics). Prefer BuildSystem for programmatic use; Topology exists so the
// historical `temporal.NewTopology(cfg).Run()` surface keeps working.
type Topology struct {
	sys *model.System
	rt  *runtime
}

// NewTopology validates cfg and builds a Topology over a model.System. The
// System is materialized lazily on the first Run.
func NewTopology(cfg Config) (*Topology, error) {
	sys, rt, err := build(cfg)
	if err != nil {
		return nil, err
	}
	return &Topology{sys: sys, rt: rt}, nil
}

// Run drives the System to its horizon and returns the run-level Result:
// the mechanism counters (Stats), the internal monitor classification
// (Status), the SLA-recovery signal (DrainMs), and the System's free-monitor
// metrics (Metrics/Digest — empty when monitoring is disabled).
func (t *Topology) Run() Result {
	if err := t.sys.Build(); err != nil {
		panic("temporal.Run: " + err.Error())
	}
	res := t.sys.Run()
	t.rt.finalize()
	return Result{
		Stats:    t.rt.stats,
		Status:   t.rt.status,
		DrainMs:  t.rt.lastDrainMs,
		Metrics:  res.Metrics,
		Digest:   res.Digest,
		SimEndMs: res.SimEndMs,
	}
}

// LastWriteMs returns the shard write-cursor position of shard idx (the
// last serialized persistence landing — a throughput proxy). 0 if idx out
// of range.
func (t *Topology) LastWriteMs(idx int) int64 { return t.rt.lastWriteMs(idx) }

// Capture returns the run's runtime mechanism recorder, or nil when capture
// was not enabled (Config.Capture false — the byte-neutral default). The
// recorder buffers in RAM during Run; the caller flushes it
// (Recorder.FlushTo) AFTER Run and folds the JSONL into a mechanism map
// with capture.Aggregate + Graph.WriteDOT/Summary. Capture output is
// excluded from the digest (observational only).
func (t *Topology) Capture() *capture.Recorder { return t.rt.cap }

// WriteDot renders the generated DOT service graph as Graphviz DOT — a
// STRUCTURAL-DEBUG artifact only. The renderer draws REGISTERED builder
// primitives; this model's real machinery is runtime-mediated and registers
// none, so the service clusters render empty. The trustworthy architecture
// picture is DERIVED FROM RUNTIME CAPTURE instead: set Config.Capture and
// the runtime records every mechanism transition at its real chokepoints
// into the recorder returned by Capture(); capture.Aggregate + Graph.WriteDOT
// then render the map from what actually traversed (keyed to the ADR-078
// component ↔ file:line table). Requires the System to be built.
func (t *Topology) WriteDot(w io.Writer) error { return t.sys.WriteDot(w) }

// System returns the underlying model.System (for inspection and
// programmatic access to the monitor/topology).
func (t *Topology) System() *model.System { return t.sys }

// ChildGate returns the run's child-partition backlog-age gate measure (the
// C1 mechanism-fidelity evidence): the per-tick tripped-fraction plus the
// per-100ms series of child head-of-backlog age. It MEASURES whether the
// isBacklogNegligible gate (Mechanism A) trips — it does NOT assert a latch.
// The gate is OBSERVATIONAL in this model (not a routing input; see
// matchingService); in this calibrated cell it stays clear (child head-age
// < 5s at the degraded aps, and the child is empty at healthy windows), so
// the recorded tripped-fraction is the honest evidence of a gate that does
// NOT engage here (see scenario doc.go). Purely observational — zero RNG,
// zero mutation, never perturbs the trace/digest. Requires Run to have
// finalized (the series is filled during ticks). A nil receiver (or an
// unrun Topology) returns zero evidence.
func (t *Topology) ChildGate() ChildGateEvidence {
	if t == nil || t.rt == nil {
		return ChildGateEvidence{}
	}
	return t.rt.ChildGate()
}

// MonitorWindows returns the internal monitor's closed-window summaries —
// the per-window series behind Result.Status (the runtime's observe.Monitor
// observes the total matching depth once per pump tick and folds it into
// 100ms windows; finalize flushes the windows at Run end). It is the
// model-side per-window backlog series that scenario code slices for
// post-hoc window analysis (e.g. the prime-trigger experiment's post-11s
// residual). The returned slice is a deep copy (observe.Monitor
// WindowSummaries) — read-only, deterministic, and byte-neutral: reading it
// mutates nothing and draws no RNG, so a caller that never reads it leaves
// the trace and digest unchanged. Empty until Run has finalized.
func (t *Topology) MonitorWindows() []observe.MonitorWindowSummary {
	if t == nil || t.rt == nil || t.rt.monitor == nil {
		return nil
	}
	return t.rt.monitor.WindowSummaries()
}

// Run is the convenience entry point: build, run, and return the Result.
// Panics on invalid configuration.
func Run(cfg Config) Result {
	top, err := NewTopology(cfg)
	if err != nil {
		panic("temporal.Run: " + err.Error())
	}
	return top.Run()
}

// BuildSystem builds the temporal model as a model.System: the matching and
// history services, whose Build closures construct the real mechanism
// (MatchQueues, poller fleets, shards, sticky) directly on the shared
// simulation, with a tick hook driving the shared runtime once per pump
// tick. The model registers NO builder ports and wires NO sys.Connect
// edges — the real data loop is runtime-mediated, so the generated DOT is
// an empty-cluster structural-debug artifact (see the package doc). The
// System owns the core.Simulation (seed from cfg) and the free monitor.
// Call sys.Build() (Run auto-builds) then sys.Run(); sys.Monitor() and
// sys.WriteDot are available after Build.
func BuildSystem(cfg Config) *model.System {
	sys, _, err := build(cfg)
	if err != nil {
		panic("temporal.BuildSystem: " + err.Error())
	}
	return sys
}

// build validates cfg, creates the runtime and the System, and wires the
// services and the tick hook. The sim is created here (not by the System)
// so the service Build closures can bind their primitives to it; the System
// adopts it via WithSim and remains the sole driver. Deterministic for a
// fixed cfg (fixed seed → identical trace).
func build(cfg Config) (*model.System, *runtime, error) {
	// ADR-122: the global run seed (DSTSEED) overrides the config seed, and it
	// is resolved BEFORE anything is derived from it — the runtime's fault
	// schedule is seeded from rt.cfg.Seed (fault_target.go), so resolving any
	// later would give one leg a sim and a fault schedule drawn from two
	// different numbers. Resolution happens here rather than in NewSystem
	// because this leg creates the sim itself and adopts it via WithSim.
	cfg.Seed = seed.Resolve(cfg.Seed)
	rt, err := newRuntime(cfg)
	if err != nil {
		return nil, nil, err
	}
	sim, err := core.NewSimulation(cfg.Seed, core.FIFOPolicy{}) //nolint:forbidigo // models/temporal owns the bind; the System adopts the sim (ADR-093)
	if err != nil {
		return nil, nil, fmt.Errorf("models/temporal: NewSimulation: %w", err)
	}
	rt.sim = sim
	sys := model.NewSystem("temporal", model.SystemConfig{
		// Seed is deliberately omitted: the adopted sim (WithSim below) is
		// the sole seed source (SystemConfig.Seed is ignored once WithSim
		// supplies the sim).
		PumpMs:         rt.pumpMs,
		SimEndMs:       rt.endMs,
		DisableMonitor: cfg.DisableMonitor,
		EnableCapture:  cfg.Capture,
	})
	sys.WithSim(sim)
	sys.AddService(matchingService(rt))
	sys.AddService(historyService(rt))
	// No cross-service sys.Connect edges: the real matching↔history data
	// loop (matching reject/drop → history transfer/reschedule → matching
	// re-add; activity retry timers re-enter matching-act) is driven by the
	// runtime mediator below, not by TaskQueue traffic through schematic
	// port edges. Declared port shells + zero-cost Connect edges would
	// render in the DOT diagram without a single task traversing them
	// (honest schematics: if the code doesn't traverse it, the diagram
	// doesn't draw it). The mechanism map derived from runtime capture
	// (Config.Capture → model/capture; see Topology.Capture) is the
	// trustworthy architecture picture.
	// The tick hook attaches the real mechanism-depth gauges on its first
	// run (the monitor exists only after Build), attaches the runtime
	// capture recorder, and then advances the runtime once per pump tick.
	var attached bool
	sys.AddTickHook(func(now core.SimTime) {
		if !attached {
			attached = true
			attachMetrics(sys, rt)
			rt.attachCap(sys.Recorder())
		}
		rt.tick(now)
	})
	return sys, rt, nil
}

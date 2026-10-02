package tasqueue

import (
	"fmt"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
	"github.com/lawRathod/queuesim/model/monitor"
	"github.com/law/GoDES/seed"
)

// Stats mirrors the Tasqueue execution counters. The queue-accounted
// counters (RejectedAdds/Dropped/Expired) and the attempt/outcome counters
// (Started/Completed/Failed/Retried/AttemptsTotal) are tallied live by the
// runtime; the DAG counters (ChainStepsDone/GroupJobsDone/OnErrorSpawned)
// count the OnSuccess/OnError machinery. For groups, Started counts one
// per group burst item and Completed counts a group once at the join (a
// group completes only if every member succeeded — getGroupStatus), so a
// GroupSize-member group run reports Started=groups, Completed=joined
// groups, and AttemptsPerStart = GroupSize× the per-member attempt rate.
type Stats struct {
	Started        uint64
	Completed      uint64 // final job in chain/group completed (or plain success)
	Failed         uint64 // exhausted MaxRetries / MaxWait expiry
	Retried        uint64
	RejectedAdds   uint64
	Dropped        uint64
	Expired        uint64
	ChainStepsDone uint64 // total chain steps completed
	GroupJobsDone  uint64
	OnErrorSpawned uint64
	AttemptsTotal  uint64 // handler attempts
}

// AttemptsPerStart returns the retry-amplification headline metric:
// handler attempts per job started (calm = 1.0 when no attempt fails and
// nothing is dropped; a drain-time drop consumes a retry without a handler
// attempt, so heavy drops push it below the failure-only expectation).
func (s Stats) AttemptsPerStart() float64 {
	if s.Started == 0 {
		return 0
	}
	return float64(s.AttemptsTotal) / float64(s.Started)
}

// Result bundles a run's outcome: the mechanism counters (Stats) joined by
// the System's free-monitor metrics (Metrics: the honest broker backlog
// gauge — queue + dispatch — the enqueue→completed latency percentile
// histogram, and the started/completed/retried/failed/dropped/expired
// counters) and their deterministic serialization (Digest), plus the
// ending sim time.
type Result struct {
	Stats Stats
	// Metrics are the System's free-monitor metrics (empty when monitoring
	// is disabled; Digest is the deterministic serialization of Metrics).
	Metrics monitor.Metrics
	Digest  string
	// SimEndMs is the ending simulation time (queue exhaustion, not the
	// configured horizon when tail events fire past it).
	SimEndMs int64
}

// Topology is the thin execution handle over a built model.System: it owns
// the System (which OWNS the core.Simulation) plus the shared runtime, and
// exposes the run-level Result surface. Prefer BuildSystem for programmatic
// use; Topology exists so the historical `tasqueue.NewTopology(cfg).Run()`
// surface keeps working.
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

// Run drives the System to its horizon and returns the run-level Result: the
// mechanism counters (Stats) plus the System's free-monitor metrics
// (Metrics/Digest — empty when monitoring is disabled).
func (t *Topology) Run() Result {
	if err := t.sys.Build(); err != nil {
		panic("tasqueue.Run: " + err.Error())
	}
	res := t.sys.Run()
	return Result{
		Stats:    t.rt.stats,
		Metrics:  res.Metrics,
		Digest:   res.Digest,
		SimEndMs: res.SimEndMs,
	}
}

// Capture returns the run's runtime mechanism recorder, or nil when capture
// was not enabled (Config.Capture false — the byte-neutral default). The
// recorder buffers in RAM during Run; the caller flushes it
// (Recorder.FlushTo) AFTER Run and folds the JSONL into a mechanism map
// with capture.Aggregate + Graph.WriteDOT/Summary. Capture output is
// excluded from the digest (observational only).
func (t *Topology) Capture() *capture.Recorder { return t.rt.cap }

// System returns the underlying model.System (for inspection and
// programmatic access to the monitor/topology).
func (t *Topology) System() *model.System { return t.sys }

// Run is the convenience entry point: build, run, and return the Result.
// Panics on invalid configuration.
func Run(cfg Config) Result {
	top, err := NewTopology(cfg)
	if err != nil {
		panic("tasqueue.Run: " + err.Error())
	}
	return top.Run()
}

// BuildSystem builds the tasqueue model as a model.System: the queue
// (broker) and worker services, whose Build closures construct the real
// mechanism (the "tasqueue:tasks" broker queue, ETA timer, poller fleet)
// directly on the shared simulation, with a tick hook driving the shared
// runtime once per pump tick. The model registers NO builder ports and
// wires NO sys.Connect edges — dispatch and completion/retry are
// runtime-mediated, so the generated DOT is an empty-cluster
// structural-debug artifact (see the package doc). The System owns the
// core.Simulation (seed from cfg) and the free monitor. Call sys.Build()
// (Run auto-builds) then sys.Run(); sys.Monitor() and sys.WriteDot are
// available after Build.
func BuildSystem(cfg Config) *model.System {
	sys, _, err := build(cfg)
	if err != nil {
		panic("tasqueue.BuildSystem: " + err.Error())
	}
	return sys
}

// build validates cfg, creates the runtime and the System, and wires the
// services and the tick hook. The sim is created here (not by the System)
// so the service Build closures can bind their primitives to it; the System
// adopts it via WithSim and remains the sole driver. Deterministic for a
// fixed cfg (fixed seed → identical trace).
func build(cfg Config) (*model.System, *runtime, error) {
	// ADR-122: the global run seed (DSTSEED) overrides the config seed, resolved
	// before the runtime is built so every seed this leg touches comes from the
	// one number. Resolution happens here rather than in NewSystem because this
	// leg creates the sim itself and adopts it via WithSim.
	cfg.Seed = seed.Resolve(cfg.Seed)
	rt, err := newRuntime(cfg)
	if err != nil {
		return nil, nil, err
	}
	sim, err := core.NewSimulation(cfg.Seed, core.FIFOPolicy{}) //nolint:forbidigo // models/tasqueue owns the bind; the System adopts the sim (ADR-093)
	if err != nil {
		return nil, nil, fmt.Errorf("models/tasqueue: NewSimulation: %w", err)
	}
	rt.sim = sim
	sys := model.NewSystem("tasqueue", model.SystemConfig{
		// Seed is deliberately omitted: the adopted sim (WithSim below) is
		// the sole seed source (SystemConfig.Seed is ignored once WithSim
		// supplies the sim).
		PumpMs:         1,
		SimEndMs:       rt.endMs,
		DisableMonitor: cfg.DisableMonitor,
		EnableCapture:  cfg.Capture,
	})
	sys.WithSim(sim)
	sys.AddService(queueService(rt))
	sys.AddService(workerService(rt))
	// No cross-service sys.Connect edges: dispatch (broker → fleet) and
	// completion/retry/chain/onError (fleet → broker) flow through the
	// runtime mediator below, not through schematic port edges. Declared
	// port shells + zero-cost Connect edges would render in the DOT diagram
	// without a single task traversing them (honest schematics: if the code
	// doesn't traverse it, the diagram doesn't draw it). The mechanism map
	// derived from runtime capture (Config.Capture → model/capture; see
	// Topology.Capture) is the trustworthy architecture picture.
	// The tick hook attaches the real mechanism metrics on its first run
	// (the monitor exists only after Build), attaches the runtime capture
	// recorder, and then advances the runtime once per pump tick.
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

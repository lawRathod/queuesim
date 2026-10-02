package temporal

import (
	"fmt"
	"math"
	"strconv"

	"github.com/law/GoDES/core"
	"github.com/law/GoDES/faultsched"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
	"github.com/lawRathod/queuesim/models/temporal/components/worker"
	"github.com/law/GoDES/observe"
)

// runtime is the shared mediator between the matching and history
// services: the real mechanism state each service's Build closure
// constructs, plus the per-pump drive that advances it. Services share one
// Go package and this runtime by design — the real Temporal services share
// internals too (ADR-093 pragmatism); the System wires the declared ports,
// owns the core.Simulation and the free monitor, and drives rt.tick once
// per pump tick.
//
// Determinism: the runtime is single-goroutine and is only mutated by pump
// tick order and scheduled events; all RNG is via substream() (fixed keys);
// no map iteration on a simulation path (fanout is indexed lookup only).
type runtime struct {
	cfg Config
	sim *core.Simulation

	// Matching (owned by MatchingService).
	wfQ     *matcher.MatchQueue // matching-wf (workflow tasks)
	actQ    *matcher.MatchQueue // matching-act ROOT partition (SDK poller fleet park & drain here)
	childQ  *matcher.MatchQueue // matching-act CHILD partition (adds land here when forwarding dead; backlog-age gate lives here)
	wfPool  *worker.Pool        // workflow-task poller fleet
	actPool *worker.Pool        // activity-task poller fleet (parks on the ROOT partition, actQ)

	// forwardReliableAct is whether the activity child partition can offload
	// to the root's poller fleet (window >= ForwardReliableMinMs). Computed
	// once at matchingService Build; drives the child-vs-root routing in
	// addActWithMode.
	forwardReliableAct bool
	// childDrainBucket bounds the child partition's own local drain (its
	// re-offer of absorbed tasks to the root at ChildDrainFraction ×
	// DispatchRate) — the drain-bound path. Deterministic TokenBucket; zero
	// draws when the child is empty (byte-neutral at healthy windows).
	childDrainBucket *matcher.TokenBucket

	// Gate measurement (the C1 mechanism-fidelity evidence): per-tick
	// counters of when the CHILD partition's backlog-age gate (Mechanism A)
	// is actually tripped, plus a per-100ms-window series of the child
	// head-of-backlog age. Read-only sim-time observations, zero RNG,
	// deterministic. Consumed by the syncmatch_sweep scenario to RECORD
	// whether the gate trips per cell — it is observational (not a routing
	// input) and does NOT latch at the calibrated aps in this cell (see
	// matchingService/doc.go); the drop is carried by Mechanism B + the
	// child's structural drain.
	childTicks     uint64
	childGateTicks uint64
	childAgeMax    int64 // max observed child backlog head-of-backlog age (ms)
	childAgeSeries []childAgeWin
	childAgeCur    childAgeWin

	// childReSpoolCount is the number of persistence re-spool round-trips
	// (write→read cycles) the child drain performed — the create-time-
	// preserving re-spool semantic (ChildReSpoolMs). Read-only observation,
	// zero RNG; exposed via ChildGate evidence. 0 when the re-spool is
	// disabled (ChildReSpoolMs = 0, byte-neutral).
	childReSpoolCount uint64

	// History (owned by HistoryService).
	shards []*history.HistoryShard
	sticky *history.StickyQueue

	retry   model.RetryController // activity retry policy (config-driven)
	monitor *observe.Monitor      // internal queue-depth/retry/completion monitor (Result.Status)

	// Workload inflow (owned by the load scheduler, load.go).
	nextID     uint64
	arrivalRNG core.RNG // nil unless ArrivalRate > 0

	// Fault/vector state (faults subpackage via fault_target.go).
	pollerForced int      // poller_min forced count; -1 = none
	wfTimerMs    int64    // timer_load per-workflow timer; -1 = none
	persistMult  float64  // persistence_stall latency multiplier; 1 = no change
	pollJitterP  float64  // poll_jitter early-expiry probability; 0 = off
	s2cMs        int64    // s2c_shorten ScheduleToClose override; 0 = config value
	jitterRNG    core.RNG // poll-jitter substream; nil unless poll_jitter active
	cascadeOn    bool     // cascade mixed-population mode (CascadeBadFraction != -1)
	// faultSchedule is the active shared fault schedule (ADR-100; nil =
	// none — the byte-neutral cfg.Vector-only path). schedActFail mirrors
	// whether the schedule's activity_failure window was active at the most
	// recent evaluation (each pump tick via applySchedule, and each
	// lifecycle decision via ActivityFailsAt/For): when a schedule is set it
	// is the SOLE activity-failure source (the config-window fallback is
	// suppressed — see fault_target.go). The field is an introspection
	// mirror; the lifecycle path decides from the per-decision pure query
	// (schedActFailAt) for exact boundary semantics.
	faultSchedule faultsched.Source
	schedActFail  bool

	// Elastic matching: active poller target per class, scaled up when
	// backlog persists (the real makePollerScalingDecisionImpl).
	pollerTargetAct int   // active activity-poller target (starts at PollersAct)
	pollerTargetWf  int   // active workflow-poller target
	backlogSinceAct int64 // when the act backlog first exceeded 0 (-1 = none)
	backlogSinceWf  int64

	// Results.
	fanout      map[uint64]int // fan-out/join: workflow ID → remaining children
	lastDrainMs int64          // last non-empty → empty transition after start; 0 = never drained
	prevDepth   int            // previous tick's total matching depth (drain-transition detector)
	stats       TopologyStats
	status      observe.MonitorStatus

	// s2s accumulates schedule-to-start latency samples for the automatic
	// S2S histogram (nil while monitoring is disabled — zero cost).
	s2s *s2sCollector

	// cap is the runtime mechanism recorder (model/capture). It is nil
	// when capture is disabled (the byte-neutral default: every record
	// site is a nil-check). When enabled it is attached on the first tick
	// from the System (which creates it at Build) — see attachCap. The
	// recorder is observational: zero RNG draws, zero I/O inside Run, and
	// never a topology or monitor node.
	cap *capture.Recorder

	pumpMs int64
	endMs  int64

	started bool // first-tick one-time inflow scheduling (start)

	// tickCount is the number of pump ticks driven so far. It rotates the
	// history-shard visit order (tick k starts at shard k%len(shards),
	// wrapping) — deterministic, zero RNG. Monotonic; never reset.
	tickCount uint64
}

// newRuntime validates and normalizes cfg and constructs the stateless
// parts of the runtime. The sim and the service-owned primitives are added
// by BuildSystem and the service Build closures. Zero-value knobs default
// sensibly; server-default mechanisms are pinned in contract_test.go.
func newRuntime(cfg Config) (*runtime, error) {
	if cfg.BurstCount <= 0 {
		return nil, fmt.Errorf("models/temporal: burst_count must be > 0 (got %d)", cfg.BurstCount)
	}
	if cfg.Vector != nil && cfg.FaultSchedule != nil {
		return nil, fmt.Errorf("models/temporal: Vector and FaultSchedule are mutually exclusive (both set)")
	}
	if cfg.BacklogGateMs < 0 || cfg.ForwardReliableMinMs < 0 || cfg.ChildDrainFraction < 0 || cfg.ChildDrainFraction > 1 || cfg.ChildReSpoolMs < 0 {
		return nil, fmt.Errorf("models/temporal: BacklogGateMs, ForwardReliableMinMs, ChildReSpoolMs must be >= 0, ChildDrainFraction in [0,1] (got %d, %d, %d, %v)", cfg.BacklogGateMs, cfg.ForwardReliableMinMs, cfg.ChildReSpoolMs, cfg.ChildDrainFraction)
	}
	// Performance seams (WP0 surface, ADR-106): every seam field must be a
	// finite value in [0,+∞) — zero = off. NaN and ±Inf are rejected on the
	// float-rate fields (NaN comparisons are false, and an infinite rate
	// would corrupt its future token bucket's refill math, so explicit
	// IsNaN/IsInf checks are required — both would otherwise slip through
	// the plain >= 0 comparisons).
	if cfg.ForwarderMaxOutstandingPolls < 0 || cfg.ForwarderMaxOutstandingTasks < 0 || cfg.AppendThreshold < 0 ||
		cfg.PollerScaleWaitMs < 0 || cfg.PollerDecisionsPerSec < 0 || math.IsNaN(cfg.PollerDecisionsPerSec) || math.IsInf(cfg.PollerDecisionsPerSec, 0) ||
		cfg.PersistenceMaxQPS < 0 || math.IsNaN(cfg.PersistenceMaxQPS) || math.IsInf(cfg.PersistenceMaxQPS, 0) ||
		cfg.PersistenceQPSBurstRatio < 0 || math.IsNaN(cfg.PersistenceQPSBurstRatio) || math.IsInf(cfg.PersistenceQPSBurstRatio, 0) {
		return nil, fmt.Errorf("models/temporal: seam fields must be >= 0 and not NaN/Inf (got polls %d tasks %d threshold %d waitMs %d decPerSec %v maxQPS %v burstRatio %v)", cfg.ForwarderMaxOutstandingPolls, cfg.ForwarderMaxOutstandingTasks, cfg.AppendThreshold, cfg.PollerScaleWaitMs, cfg.PollerDecisionsPerSec, cfg.PersistenceMaxQPS, cfg.PersistenceQPSBurstRatio)
	}
	if cfg.PollersWF <= 0 || cfg.PollersAct <= 0 {
		return nil, fmt.Errorf("models/temporal: pollers must be > 0 (wf %d act %d)", cfg.PollersWF, cfg.PollersAct)
	}
	if cfg.SimEndMs <= 0 {
		cfg.SimEndMs = 60000
	}
	if cfg.PumpMs <= 0 {
		cfg.PumpMs = 1
	}
	if cfg.LongPollMs <= 0 {
		cfg.LongPollMs = 60000 // server default (MatchingLongPollExpirationInterval)
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.PollerScaleUpBacklogMs <= 0 &&
		(cfg.PollerScaleMax > cfg.PollersAct || cfg.PollerScaleMax > cfg.PollersWF) {
		cfg.PollerScaleUpBacklogMs = 200 // server default (MatchingPollerScalingBacklogAgeScaleUp)
	}
	return &runtime{
		cfg:             cfg,
		faultSchedule:   cfg.FaultSchedule,
		fanout:          map[uint64]int{},
		pumpMs:          cfg.PumpMs,
		endMs:           cfg.SimEndMs,
		pollerForced:    -1,
		cascadeOn:       cfg.CascadeBadFraction != -1, // -1 = cascade off
		pollerTargetAct: cfg.PollersAct,
		pollerTargetWf:  cfg.PollersWF,
		backlogSinceAct: -1,
		backlogSinceWf:  -1,
		persistMult:     1, // no persistence stall by default
		pollJitterP:     0,
		wfTimerMs:       -1,
		retry: model.RetryController{
			InitialIntervalMs: cfg.RetryInitialMs,
			Coefficient:       cfg.RetryCoeff,
			MaxAttempts:       cfg.RetryMaxAttempts,
		},
		monitor: observe.NewMonitor(model.MonitorConfig(), 0),
	}, nil
}

// rescheduleBackoffMs returns the queues-framework reschedule-policy
// backoff before the given 1-based attempt. The server's generic policy is
// 1s initial, 1.1× coefficient, 3min cap — pinned to the submodule in
// contract_test.go (temporal/common/util.go:67-69). Retry policy, not a
// fault: lives here next to the runtime's RetryController.
func rescheduleBackoffMs(attempt int32) int64 {
	const initial = 1000 // ms — taskRescheduleInitialInterval
	const coeff = 1.1    // taskRescheduleBackoffCoefficient
	const capMs = 180000 // 3min — taskRescheduleMaxInterval
	if attempt <= 1 {
		return initial
	}
	v := float64(initial) * math.Pow(coeff, float64(attempt-1))
	if math.IsNaN(v) || math.IsInf(v, 1) || v >= float64(capMs) {
		return capMs
	}
	ms := int64(math.Round(v))
	if ms > capMs {
		ms = capMs
	}
	if ms < initial {
		ms = initial
	}
	return ms
}

// tick is the per-pump drive, registered on the System as a tick hook. It
// runs once per pump tick at a fixed position (after the free monitor's
// sample, before child recursion) and advances every mechanism in the real
// service order: history shards (transfer/timer/rescheduler) → matching
// poller fleet → sticky drain (sticky-first) → class dispatch → fault-vector
// knobs → depth/drain bookkeeping.
func (rt *runtime) tick(now core.SimTime) {
	nowMs := now.Int64()
	if nowMs >= rt.endMs {
		return
	}
	if !rt.started {
		rt.started = true
		rt.start(now)
	}
	// 1. History shards: transfer + timer processors, rescheduler. The
	// visit order rotates deterministically (tick k starts at shard
	// k%len(shards), wrapping) — zero RNG. Due reschedules re-enter
	// matching via onResched (scheduler submit through the add boundary),
	// never the transfer queue (the shard's legacy fallback only applies
	// when the hook is omitted).
	n := len(rt.shards)
	start := 0
	if n > 0 {
		start = int(rt.tickCount % uint64(n))
	}
	// The hooks capture only rt and the tick-invariant now, so they are
	// built once per tick (not per shard).
	onXfer := func(tk Task) { rt.onTransfer(tk, now) }
	onTmr := func(tk Task) { rt.onTimer(tk, now) }
	onResched := func(tk Task) { rt.onResched(tk, now) }
	for i := 0; i < n; i++ {
		sh := rt.shards[(start+i)%n]
		sh.Tick(now, onXfer, onTmr, onResched)
	}
	rt.tickCount++
	// 2. Workers: keep the required poller count parked (long-poll).
	rt.maintainWorkers(nowMs)
	// 3. Matching dispatch: a worker's poll checks its sticky queue first
	// (sticky-first), then the class backlogs. The activity child partition's
	// own local drain re-offers absorbed tasks to the ROOT before the root
	// dispatches, so root-drained tasks reach pollers within the same tick.
	rt.dispatchSticky()
	rt.dispatchFrom(rt.wfQ)
	rt.drainChildToRoot(now)
	rt.dispatchFrom(rt.actQ)
	// 4. Fault-vector knobs.
	rt.applyVector(nowMs)
	// 4.5 Gate measurement: sample the child partition's backlog-age gate
	// once per tick (the mechanism-fidelity evidence RECORDING whether the
	// gate trips — observational; it does not latch at the calibrated aps in
	// this cell, see matchingService/doc.go). Purely observational;
	// zero RNG, zero sim mutation — never perturbs the trace/digest.
	rt.observeChildGate(now)
	// 5. Depth + DrainMs (the SLA-recovery signal). DrainMs records the
	// last time the backlog TRANSITIONED from non-empty to empty after the
	// run started — 0 means it never drained. Pre-burst empty time is not
	// a drain.
	depth := rt.wfQ.Len() + rt.actQ.Len() + rt.sticky.Len() + rt.childQ.Len()
	rt.monitor.ObserveQueueLen(nowMs, 0, uint64(depth))
	if depth > rt.stats.MaxQueue {
		rt.stats.MaxQueue = depth
	}
	if rt.stats.Started > 0 {
		if depth == 0 && rt.prevDepth > 0 {
			rt.lastDrainMs = nowMs
		}
		rt.prevDepth = depth
	}
}

// onResched processes a due rescheduled task (the queues-framework
// rescheduler — temporal/queues/executable.go Nack → reschedule): the task
// re-enters matching DIRECTLY via the add boundary (scheduler submit) in
// the same tick, never the transfer queue. Routing mirrors the legacy
// transfer-hop destination (onTransfer's Rescheduled branch): activity
// retries re-enter on the SPOOL path (no sync-match, matching the server's
// backoff-rescheduled tasks), workflow tasks via addWf. Same-tick submit
// (not next-tick transfer drain) is the intended timing change versus the
// legacy fallback.
func (rt *runtime) onResched(tk Task, now core.SimTime) {
	switch tk.Class {
	case TaskWfStart, TaskWfComplete:
		// Capture: history → matching — a due reschedule re-enters
		// matching-wf directly (scheduler submit, not transfer).
		rt.rec("history", "matching", "resched_wf", now.Int64(), tk)
		rt.addWf(tk, now.Add(rt.cfg.NetworkMs))
	case TaskActivity:
		// Capture: history → matching — a due activity reschedule
		// re-enters matching-act on the spool path.
		rt.rec("history", "matching", "resched_act", now.Int64(), tk)
		rt.addActSpoolOnly(tk, now.Add(rt.cfg.NetworkMs))
	default:
		rt.rec("history", "matching", "resched_act", now.Int64(), tk)
		rt.addAct(tk, now.Add(rt.cfg.NetworkMs))
	}
}

// start runs once, on the first pump tick, before any task can reach the
// matching queues: it parks the long-poll pollers, applies the
// construction-time fault-vector knobs (timer_load must be active before
// the burst's startWorkflow — the duplicate applyVector at step 4 of the
// same tick is intentional and idempotent), and schedules the inflow. The
// first add reaches matching only after the full start → persist →
// transfer → matching chain (a few ms), so parking on the first tick is
// observably equivalent to the old hand-wired t=0 parking.
func (rt *runtime) start(now core.SimTime) {
	nowMs := now.Int64()
	rt.parkUpTo(rt.wfPool, rt.wfQ, rt.cfg.PollersWF, nowMs)
	rt.parkUpTo(rt.actPool, rt.actQ, rt.cfg.PollersAct, nowMs)
	rt.applyVector(nowMs)
	rt.scheduleBurst(now)
	rt.scheduleBacklogSurge(now)
	if rt.cfg.ArrivalRate > 0 {
		rt.arrivalRNG = substream(rt.sim, "arrival")
		rt.scheduleArrival()
	}
	if rt.cfg.ArrivalSwitchMs > 0 {
		rt.sim.ScheduleAt(core.SimTime(rt.cfg.ArrivalSwitchMs), func(*core.Simulation) {
			rt.cfg.ArrivalRate = rt.cfg.ArrivalRate2
		})
	}
}

// finalize closes the run: flush the internal monitor windows, capture the
// status classification, and aggregate the queue counters into TopologyStats.
func (rt *runtime) finalize() {
	rt.monitor.FlushWindows(rt.endMs)
	rt.status = rt.monitor.Status()
	rt.finalizeStats()
}

// lastWriteMs returns the shard write-cursor position of shard idx (the
// last serialized persistence landing — a throughput proxy). 0 if idx out
// of range.
func (rt *runtime) lastWriteMs(idx int) int64 {
	if idx < 0 || idx >= len(rt.shards) {
		return 0
	}
	return rt.shards[idx].LastWriteMs()
}

// attachCap wires the System's runtime mechanism recorder into the runtime.
// Called once, on the first pump tick (the System creates the recorder at
// Build). Nil when capture is disabled — every record site nil-checks, so
// the off-path is byte-neutral. Deterministic under a fixed seed: Record is
// only ever called from single-goroutine sim order.
func (rt *runtime) attachCap(c *capture.Recorder) {
	rt.cap = c
}

// rec records one observed mechanism transition when capture is enabled.
// Component names are the model's own ("load", "history", "matching",
// "exec"); kind is the transition category. rec is a no-op when capture is
// off (nil recorder — byte-neutral).
func (rt *runtime) rec(from, to, kind string, nowMs int64, tk Task) {
	if rt.cap == nil {
		return
	}
	rt.cap.Record(from, to, kind, nowMs, capture.WithTask(taskIDString(tk.ID), int(tk.Attempt), int(tk.Class)))
}

// taskIDString renders a numeric task ID as the capture TaskID label.
func taskIDString(id uint64) string {
	return strconv.FormatUint(id, 10)
}

// attachMetrics registers the automatic free-monitor metrics on the
// System's monitor (the LIVE MatchQueue depths and poller counts, the
// history shards' per-surface backlogs, the worker in-flight counts, the
// throughput/outcome counters, and the S2S latency histogram). The
// closures are pure reads of runtime state — zero RNG draws, zero
// mutation — so the monitor stays determinism-neutral (on/off runs are
// bit-identical). No-op when monitoring is disabled. Called once, on the
// first pump tick (the monitor exists only after Build; the first Sample
// happens before this, so the metrics join from the second tick onward —
// deterministic).
func attachMetrics(sys *model.System, rt *runtime) {
	m := sys.Monitor()
	if m == nil {
		return
	}
	rt.s2s = &s2sCollector{}
	m.AddHistogram(MetricS2SLatency, func() []int64 {
		batch := rt.s2s.samples
		rt.s2s.samples = nil
		return batch
	})
	m.AddCounter(MetricStarted, func() uint64 { return rt.stats.Started })
	m.AddCounter(MetricActivityAdds, func() uint64 { return rt.stats.ActivityAdds })
	m.AddCounter(MetricCompleted, func() uint64 { return rt.stats.Completed })
	m.AddCounter(MetricRetries, func() uint64 { return rt.stats.Retries })
	m.AddCounter(MetricFailed, func() uint64 { return rt.stats.Failed })
	m.AddGauge("matching.wf.depth", func() int64 { return int64(rt.wfQ.Len()) })
	m.AddGauge("matching.act.depth", func() int64 { return int64(rt.actQ.Len()) })
	m.AddGauge("matching.wf.pollers", func() int64 { return int64(rt.wfQ.Parked()) })
	m.AddGauge("matching.act.pollers", func() int64 { return int64(rt.actQ.Parked()) })
	m.AddGauge("matching.wf.inflight", func() int64 { return int64(rt.wfPool.InFlight()) })
	m.AddGauge("matching.act.inflight", func() int64 { return int64(rt.actPool.InFlight()) })
	m.AddGauge("matching.sticky.depth", func() int64 { return int64(rt.sticky.Len()) })
	// NB: history.shards.inflight is MISNAMED — it sums queue DEPTH
	// (LenTransfer+LenTimer+LenResched), not in-flight work. The shard has no
	// per-tick worker effect (components/history/shard.go), so there is no
	// in-flight quantity to report. The name is kept for digest stability
	// (renaming would move every pinned digest — ADR-115); under
	// Config.LabeledMetrics the correctly-named per-shard depth gauge
	// (history.shards.queue_depth{shard,kind}) is additionally registered.
	m.AddGauge("history.shards.inflight", func() int64 {
		var n int
		for _, sh := range rt.shards {
			n += sh.LenTransfer() + sh.LenTimer() + sh.LenResched()
		}
		return int64(n)
	})
	m.AddGauge("history.shards.transfer", func() int64 {
		var n int
		for _, sh := range rt.shards {
			n += sh.LenTransfer()
		}
		return int64(n)
	})
	m.AddGauge("history.shards.timer", func() int64 {
		var n int
		for _, sh := range rt.shards {
			n += sh.LenTimer()
		}
		return int64(n)
	})
	m.AddGauge("history.shards.resched", func() int64 {
		var n int
		for _, sh := range rt.shards {
			n += sh.LenResched()
		}
		return int64(n)
	})
	// Opt-in labeled/attributed surfaces (ADR-115). Off by default: nothing
	// new is registered and the pinned digests do not move.
	if rt.cfg.LabeledMetrics {
		rt.attachLabeledMetrics(m)
	}
}

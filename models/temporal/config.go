package temporal

import (
	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/faultsched"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
)

// TaskClass distinguishes the task kinds that traverse the Temporal
// lifecycle (canonical type: matcher.TaskClass).
type TaskClass = matcher.TaskClass

const (
	// TaskWfStart is the initial workflow-task.
	TaskWfStart = matcher.TaskWfStart
	// TaskActivity is an activity-task.
	TaskActivity = matcher.TaskActivity
	// TaskWfComplete is the post-activity workflow-task.
	TaskWfComplete = matcher.TaskWfComplete
)

// Task is a unit of work flowing through the temporal network (canonical
// type: matcher.Task).
type Task = matcher.Task

// TaskExpireStage aliases the matcher expiry stage.
type TaskExpireStage = matcher.TaskExpireStage

const (
	ExpireNone    = matcher.ExpireNone
	ExpireAtSpool = matcher.ExpireAtSpool
	ExpireAtRead  = matcher.ExpireAtRead
	ExpireAtPoll  = matcher.ExpireAtPoll
)

// Config is the temporal model configuration. Every mechanism knob has a
// zero default that disables it (neutrality contract); fields with
// non-zero defaults are applied in newRuntime. All latencies are sim ms.
type Config struct {
	// Seed seeds the System-owned Simulation; a fixed seed yields a
	// bit-identical trace (determinism contract).
	Seed        int64
	BurstCount  int
	BurstAtMs   int64
	ArrivalRate float64 // workflows/s after the burst; 0 = none
	// ArrivalRate2 + ArrivalSwitchMs enable a TIME-VARYING arrival rate:
	// sustained ArrivalRate until ArrivalSwitchMs, then ArrivalRate2. This
	// drives history-dependent hysteresis (collapse at a high rate, then
	// load drops mid-run while the backlog is saturated). ArrivalRate2 = 0
	// = stop arrivals (off).
	ArrivalRate2    float64
	ArrivalSwitchMs int64

	// Frontend / transport
	NetworkMs  int64 // per-RPC propagation (default 2)
	ActivityMs int64 // per-activity service time (0 = instant)
	// FanoutK is the fan-out width: on activity success the workflow spawns
	// FanoutK child activities (0 = no fan-out — the single-activity
	// lifecycle). A failing parent retries and re-spawns all children on
	// success.
	FanoutK int
	// CascadeBadFraction is the fraction of workflows (by deterministic
	// workflow-ID hash) whose activities FAIL during the default fault
	// window — the bad actors. Healthy workflows never fail. Both share the
	// same queues, so bad-actor retries cascade onto the healthy majority.
	// -1 = off (all workflows follow the global fault predicate); 0..1 = fraction.
	CascadeBadFraction float64

	// Fault window (default activity-failure window)
	FaultStartMs int64
	FaultEndMs   int64 // 0 = open-ended

	// Matching
	MatchingCapacity int   // backlog capacity per class queue; 0 = unbounded
	PollersWF        int   // workflow-task pollers
	PollersAct       int   // activity-task pollers
	PollerBudget     int   // in-flight cap per poller (0 = unlimited)
	LongPollMs       int64 // long-poll expiration (server default 1min; 0 = keep-polling-until-stop)
	// Elastic matching: when the matching backlog persists past
	// PollerScaleUpBacklogMs, the active poller count scales UP toward
	// PollerScaleMax (the real server's makePollerScalingDecisionImpl —
	// backlog age > PollerScalingBacklogAgeScaleUp → +1 poller, see
	// contract_test.go). PollerScaleMax <= PollersWF/Act disables scaling
	// (fixed fleet). PollerScaleUpBacklogMs defaults to the server's 200ms
	// when scaling is enabled (matching.pollerScalingMinimumBacklog).
	PollerScaleMax         int   // max active pollers per class; <= configured = off
	PollerScaleUpBacklogMs int64 // backlog age that triggers scale-up (server default 200ms)

	// Dispatch throttle (token bucket)
	DispatchRate  float64 // tokens/s; 0 = unlimited
	DispatchBurst int     // bucket capacity; default 1

	// SyncMatchWindowMs is the matching sync-match window (server default
	// 200ms, MatchingSyncMatchWaitDuration). 0 = disabled (sync-match
	// whenever a poller is parked — the pre-fix behavior). A non-zero
	// window makes adds spool to the backlog once pollers have been parked
	// longer than the window, which is what lets the backlog build under
	// sustained load (the add_drop and S2S amplification mechanism).
	SyncMatchWindowMs int64

	// BacklogGateMs (Mechanism A — the isBacklogNegligible gate) is the
	// head-of-backlog AGE threshold in ms at which the foreground sync-match
	// fast path is disabled and adds route to the local spooled backlog —
	// the real server's backlog-disable gate (matcher.go:109-116; threshold
	// getBacklogAge() < 5s, dynamicconfig/constants.go:1277-1279). 0 =
	// disabled (no gate).
	BacklogGateMs int64

	// ForwardReliableMinMs (Mechanism B — window-mediated forwarding) is the
	// minimum SyncMatchWindowMs at which a partition can offload tasks to a
	// root poller. A window shorter than this kills forwarding (the real
	// TrySyncMatch builds an instantly-expired child context, common/
	// contextutil/deadline.go:34-38) → adds stay local and accumulate →
	// the backlog trips BacklogGateMs. Default = the real sync-match default
	// (200ms): at the healthy default the forward path is reliable;
	// a below-default window is unreliable. 0 = forwarding always reliable
	// (no B gate).
	ForwardReliableMinMs int64

	// ChildDrainFraction is the CHILD write-partition's structural local
	// drain capacity, expressed as a fraction of DispatchRate. The activity
	// path is modeled as TWO distinct partitions with distinct drain
	// capacity: a CHILD (write/persistence) partition that absorbs history
	// adds (rt.childActQ — the isBacklogNegligible backlog-age gate lives
	// here) and a ROOT (read/polled) partition that the SDK poller fleet
	// drains (rt.actQ). When the child CAN forward (healthy window) each add
	// is offered straight to the root's pollers — byte-neutral, the root
	// behaves exactly like the collapsed single queue. When forwarding is
	// dead (short/no window) adds are absorbed into the child and can only
	// leave via the child's OWN drain (a bounded re-offer to the root) at
	// ChildDrainFraction × DispatchRate — the drain-bound residual. This is
	// a STRUCTURAL property (one write-partition cannot offload to the root's
	// full poller fleet, so it drains at its own share), engaged only when
	// forwarding fails; it is NOT a window-conditional rate multiplier like
	// the retired ForwardDeadRateFactor proxy. 1.0 = child drains at the
	// full rate (no slowdown — no drop); the default is calibrated to the
	// real server's window-0 successful-add drop (~30%, bubble c2@0 7.33 vs
	// c1@200 10.50).
	ChildDrainFraction float64

	// ChildReSpoolMs (the persistence RE-SPOOL ROUND-TRIP; 2026-09-07) is
	// the write→read latency of the CHILD write-partition's durable queue —
	// the missing create-time-preserving persistence hop that a
	// memory-resident FIFO drain (the pre-re-spool model) cannot express.
	// When > 0, a child task drained toward the ROOT is first WRITTEN to
	// persistence and READ BACK (a ChildReSpoolMs hop); its create-time
	// (BornMs) is preserved throughout, so the task's age (now − BornMs,
	// the gate's head-of-backlog test) keeps counting while it is in the
	// persistence round-trip and not servable. If, at read-back, the root
	// has no parked poller to serve it immediately (the storm-congestion
	// state), the task RE-SPOOLS (another round-trip, age still preserved)
	// rather than queueing — so an old head can exceed BacklogGateMs even
	// while the drain proceeds on fresh tasks, decoupling THROUGHPUT from
	// HEAD-AGE (the real server's getBacklogAge = min create-time across
	// ALL backlogged tasks: matcher.go:540-542, constants.go:1277-1279).
	// 0 = disabled: the child drains exactly as before (byte-neutral — the
	// current immediate-FIFO re-offer to the root).
	// PollForwardEnabled controls the child→root poll-forwarding path
	// (matching_scale.go pollForward: a child-partition backlog borrows an idle
	// ROOT poller, one head per tick). Real Temporal has poll-forwarding,
	// but the model's unconditional 1-per-tick approximation is UNCALIBRATED
	// and inflates the degraded-window attempt count (c2 aps 7.28→9.46 vs
	// the real-server bubble 7.33). It is therefore OFF by default; enable
	// only with an explicit recalibration.
	PollForwardEnabled bool

	ChildReSpoolMs int64

	// History shards
	//
	// Pin vs calibration labels (Track 4): TransferBatch, TransferWorkers,
	// TimerBatch, TimerWorkers, TimerMaxShiftMs, and ReschedulerBackoffMs
	// are SOURCE PINS (file:line refs on each field); PersistMs is an
	// explicit NON-PIN (no server default claimed — 0 = none).
	Shards int // range-owned shards (default 1)
	// PersistMs is the persistence write latency per mutation. 0 = none.
	// There is deliberately NO source pin here: the model does not claim a
	// server default for persistence latency (explicit non-pin).
	PersistMs int64
	// TransferBatch is the transfer-queue reader batch size. CALIBRATION
	// OVER PIN: the server default is 100 (TransferTaskBatchSize,
	// temporal/common/dynamicconfig/constants.go:1867-1871), but the serial
	// 4-poller model cannot compose the server's 512 parallel transfer
	// workers — applying 100 makes all workflows start in one tick (calm
	// spools, S2S>0). The effective default is therefore the calibrated 1
	// (one transfer/tick); 100 is a documented reference, not an applied
	// default.
	TransferBatch int
	// TransferWorkers is the transfer-queue processor worker count (server
	// default 512 — TransferProcessorSchedulerWorkerCount,
	// temporal/common/dynamicconfig/constants.go:1882-1886). Carried for
	// fidelity; the serial model drains by batch only (shard.go).
	TransferWorkers int
	// TimerBatch is the timer-queue reader batch size (server default 100 —
	// TimerTaskBatchSize,
	// temporal/common/dynamicconfig/constants.go:1790-1794).
	TimerBatch int
	// TimerWorkers is the timer-queue processor worker count (server default
	// 512 — TimerProcessorSchedulerWorkerCount,
	// temporal/common/dynamicconfig/constants.go:1795-1799). Carried for
	// fidelity; the serial model drains by batch only (shard.go).
	TimerWorkers int
	// TimerMaxShiftMs is the timer time-shift cap (server default 1s —
	// TimerProcessorMaxTimeShift,
	// temporal/common/dynamicconfig/constants.go:1845-1849; 0 = no cap).
	TimerMaxShiftMs int64
	// ReschedulerBackoffMs is the rescheduler backoff after a failed
	// execution (server default 1s — taskRescheduleInitialInterval,
	// temporal/common/util.go:67; 0 = immediate).
	ReschedulerBackoffMs int64

	// Retry
	RetryInitialMs   int64
	RetryCoeff       float64
	RetryMaxAttempts int32 // 0 = unlimited
	// RetryMaximumIntervalMs is the activity-retry backoff ceiling. 0 =
	// the legacy 60s ceiling (model.RetryController's zero-value
	// maxBackoffMs — byte-neutral); a positive value is an explicit cap.
	// Default 0 (neutral — the field is config surface; the runtime
	// threads it into its RetryController in a later track).
	RetryMaximumIntervalMs int64
	ScheduleToCloseMs      int64 // activity schedule-to-close cap; 0 = none

	// Timeouts
	WorkflowTaskTimeoutMs int64 // wf-task schedule-to-start (server default 10s; 0 = none)
	ActivityS2STimeoutMs  int64 // activity schedule-to-start (0 = none)
	HeartbeatTimeoutMs    int64 // activity heartbeat timeout (0 = none)

	// Sticky
	StickyEnabled    bool  // route TaskWfComplete via sticky queues
	StickyCacheMs    int64 // worker sticky-cache residency (0 = forever)
	StickyFallbackMs int64 // sticky-worker-unavailable fallback delay
	// StickyS2STimeoutMs is the sticky schedule-to-start timeout: a sticky
	// task older than this falls back to the normal queue (see
	// history.StickyQueue.PollExpired). <= 0 = disabled (neutral — sticky
	// tasks never S2S-expire, today's behavior). Reference horizon 10s —
	// DefaultWorkflowTaskTimeout
	// (temporal/common/primitives/constants.go:16, wired as the namespace
	// default at temporal/common/dynamicconfig/constants.go:2363-2366).
	// Default 0 (neutral — no caller threads this yet; opt-in per task).
	StickyS2STimeoutMs int64

	// Performance seams — Phase 1 (four grounded performance seams, ADR-106;
	// plan 2026-09-07-four-grounded-performance-seams-phase1.md). WP0 adds
	// the seven zero-default fields below + validation ONLY; seam behavior
	// lands in WP1-WP4, each gated on its field != 0 (0 = off = today's
	// exact behavior — the neutrality contract, proven in seams_test.go).
	// Server defaults pinned to the temporal submodule at ffddf6fdc.

	// ForwarderMaxOutstandingPolls caps the child→root forwarder's inflight
	// POLLS (S1 — server default 1, matching.forwarderMaxOutstandingPolls,
	// common/dynamicconfig/constants.go:1220). 0 = off = unbounded (today).
	// WP1 enforces the cap as backpressure (queue, not drop) on the forward
	// seam — a single-seam approximation: the model has no per-partition
	// structure, so the cap applies at the one forward seam.
	ForwarderMaxOutstandingPolls int
	// ForwarderMaxOutstandingTasks caps the child→root forwarder's inflight
	// add/query tasks (S1 — server default 1,
	// matching.forwarderMaxOutstandingTasks,
	// common/dynamicconfig/constants.go:1225). 0 = off = unbounded (today).
	ForwarderMaxOutstandingTasks int
	// AppendThreshold is the matching task-writer append-channel cap (S2 —
	// server default 250, matching.outstandingTaskAppendsThreshold,
	// common/dynamicconfig/constants.go:1161). 0 = off (no reject-at-cap:
	// today's unbounded persistence-backed matching). WP2 rejects at cap
	// with escalation-backoff re-add; the model leg stays partial per
	// ADR-103 (bubble-first).
	AppendThreshold int
	// PollerScaleWaitMs is the poller-scaling WAIT gate (S3 — server default
	// 1s, matching.pollerScalingWaitTime,
	// common/dynamicconfig/constants.go:1360). 0 = off = today's +1/tick
	// past the backlog age. WP3 gates scaleElastic on it. Remap (explicit
	// divergence): the real WaitTime triggers scale-down on idle ≥1s; the
	// model reuses the knob to space UP decisions ≥1s apart (approximation;
	// relief cadence at server defaults ~1/s vs the real up-decision bound
	// 10/s×pollers).
	PollerScaleWaitMs int64
	// PollerDecisionsPerSec is the poller-scaling decisions-per-second cap
	// (S3 — server default 10, matching.pollerScalingDecisionsPerSecond,
	// common/dynamicconfig/constants.go:1366). 0 = off. WP3 enforces it with
	// a token bucket.
	PollerDecisionsPerSec float64
	// PersistenceMaxQPS is the persistence rate ceiling (S4 — server defaults
	// frontend 2000 / matching 3000 / history 9000,
	// frontend/matching/history.persistenceMaxQPS,
	// common/dynamicconfig/constants.go:585/1080/1485; the model carries ONE
	// ceiling at its single persistence-touching choke point). 0 = off =
	// unbounded (today's behavior). WP4 applies a token bucket on the
	// persistence dispatch path.
	PersistenceMaxQPS float64
	// PersistenceQPSBurstRatio is the persistence token-bucket burst ratio
	// (S4 — server default 1.0, system.persistenceQPSBurstRatio,
	// common/dynamicconfig/constants.go:251). Ratio 0 = no burst credit: every
	// write is paced at QPS when the max-QPS ceiling is on (the server's
	// burst-0 limiter behavior — PersistenceMaxQPS is the master switch: a
	// ratio only shapes a live ceiling). The effective burst
	// (QPS × ratio) is pinned from CODE behavior in WP4, not from a comment
	// (CODE-not-comment, ADR-106:3). Must be >= 0 (validation); negative,
	// NaN, and Inf rejected.
	PersistenceQPSBurstRatio float64

	PumpMs   int64 // topology pump cadence (default 1)
	SimEndMs int64 // horizon (default 60000)

	// DisableMonitor turns OFF the System's free monitor. Monitoring is
	// read-only and never perturbs the simulation, so an on/off system
	// yields a bit-identical simulation trace (determinism gate).
	DisableMonitor bool

	// LabeledMetrics turns ON the OPT-IN labeled/attributed metric surfaces
	// (per history shard, per matching queue). FALSE by default: registers
	// NOTHING new, so the six pinned determinism digests are unmoved; the
	// labeled registration path is the one to use for attribution figures.
	// TRUE additionally registers the load-vocabulary surfaces (model/monitor
	// LoadName/load vocabulary) — pure reads, zero RNG draws, zero mutation,
	// so the simulation trace stays bit-identical (labeled_neutrality_test.go
	// proves the labeled-on run's unlabeled-equivalent projection equals the
	// flag-off digest on an RNG-drawing storm). ADR-115.
	LabeledMetrics bool

	// Capture turns ON the runtime mechanism recorder (model/capture).
	// The model records transitions between its own component names at its
	// real chokepoints (history/matching/exec/load); capture is
	// observational only — zero RNG draws, zero I/O inside Run, and never
	// registered as a topology or monitor node — so capture on/off leaves
	// the simulation trace and digest byte-identical. When enabled, flush
	// the recorder after Run (System().Recorder().FlushTo).
	Capture bool

	Vector *model.DecisionVector // active decision vector; nil = none

	// FaultSchedule is the active shared fault schedule (ADR-100): when
	// non-nil, applyVector queries it each pump tick (State(seed, nowMs))
	// and the returned faults feed the SAME faults.Target dispatcher as the
	// vector path — the schedule's per-kind windows reproduce today's
	// vector semantics BY CONSTRUCTION (max-rate for the drop kinds,
	// last-wins for the scalar kinds, direct apply for
	// persistence_stall/poll_jitter/s2c_shorten). nil = none: exactly
	// today's cfg.Vector behavior, byte-identical. cfg.Vector and
	// FaultSchedule are MUTUALLY EXCLUSIVE (both set → validation error,
	// fail-fast and deterministic). The schedule is also the SOLE
	// activity-failure source when set — the config-window fallback
	// (FaultStartMs/FaultEndMs) does NOT apply under a schedule.
	FaultSchedule faultsched.Source

	// ScheduleDecisionLog is the OPTIONAL observational per-query
	// fault-schedule decision recorder (ADR-112). When non-nil,
	// applySchedule records every State decision (the raw nowMs + the
	// active set) into it. nil (the default) = off. It is purely
	// observational: it never feeds a decision, draws no RNG, and is
	// excluded from Result.Digest — so setting it leaves the trace and
	// digest byte-identical (the WP5x neutrality gate). Off by default.
	ScheduleDecisionLog *faultsched.Coverage
}

// DefaultConfig returns the configuration with server-default mechanisms
// where the real system defines them (200ms sync-match window, 1min
// long-poll, 1s/1.1×/3min reschedule policy, 100-task transfer/timer
// batches, 512 transfer/timer workers, 1s timer max-shift, 1s rescheduler
// backoff — see contract_test.go for the submodule pins) and the historical
// calibration elsewhere (ChildDrainFraction; PersistMs claims no server
// default). Zero-value knobs stay neutral (RetryMaximumIntervalMs 0 =
// legacy 60s ceiling, StickyS2STimeoutMs 0 = no sticky S2S expiry).
func DefaultConfig() Config {
	return Config{
		Seed:                 42,
		BurstCount:           50,
		BurstAtMs:            0,
		NetworkMs:            2,
		MatchingCapacity:     60,
		PollersWF:            4,
		PollersAct:           4,
		PollerBudget:         1,
		LongPollMs:           60000,
		DispatchBurst:        1,
		SyncMatchWindowMs:    200,  // server default (MatchingSyncMatchWaitDuration)
		BacklogGateMs:        5000, // real isBacklogNegligible threshold (5s)
		ForwardReliableMinMs: 200,  // forward reliable at/above the real sync-match default
		ChildDrainFraction:   0.75, // child write-partition local drain = 75% of DispatchRate (window-0 drop; bubble c2@0 mean 7.33)
		Shards:               1,
		TransferBatch:        1,    // CALIBRATION OVER PIN: the serial 4-poller model cannot compose the server's 512-parallel-worker transfer drain, so batch 1 (one transfer/tick) is the calibrated default; the server reference 100 (TransferTaskBatchSize) is documented on the field but NOT applied. Applying 100 made calm spool and S2S>0 (all workflows start in one tick).
		TransferWorkers:      512,  // server default (TransferProcessorSchedulerWorkerCount)
		TimerBatch:           100,  // server default (TimerTaskBatchSize)
		TimerWorkers:         512,  // server default (TimerProcessorSchedulerWorkerCount)
		TimerMaxShiftMs:      1000, // server default (TimerProcessorMaxTimeShift 1s)
		ReschedulerBackoffMs: 1000, // server default (taskRescheduleInitialInterval 1s)
		RetryInitialMs:       100,
		RetryCoeff:           1.0,
		PumpMs:               1,
		SimEndMs:             60000,
		CascadeBadFraction:   -1, // cascade off by default
	}
}

// substream returns the deterministic RNG substream for a model feature.
// The path keeps the historical "model.temporal.temporal." namespace
// (intentional binder isolation, ADR-091); the key is constructor-fixed —
// late mutation would silently change the draw sequence and break
// determinism.
func substream(sim *core.Simulation, name string) core.RNG {
	return sim.Substream("model.temporal.temporal." + name)
}

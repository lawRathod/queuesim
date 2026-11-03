package tasqueue

import (
	"strconv"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
)

// Automatic monitor metric names. These are registered on the System's
// free monitor by attachMetrics when monitoring is enabled — zero model
// configuration. They flow into Result.Metrics, where the monitor.Metrics
// finders (Counter/Gauge) surface them by name.
const (
	// MetricStarted counts jobs entered (initial burst + scheduled/cron
	// bursts).
	MetricStarted = "tasqueue.started"
	// MetricCompleted counts successful job completions (chain tail, group
	// join, or plain success).
	MetricCompleted = "tasqueue.completed"
	// MetricRetried counts failed attempts re-enqueued immediately
	// (retryJob).
	MetricRetried = "tasqueue.retried"
	// MetricFailed counts jobs that exhausted their retry budget or
	// expired in the queue.
	MetricFailed = "tasqueue.failed"
	// MetricDropped counts drain-time fault drops.
	MetricDropped = "tasqueue.dropped"
	// MetricExpired counts MaxWaitMs deadline expiries (terminal).
	MetricExpired = "tasqueue.expired"
	// MetricQueueDepth is the REAL pending-job backlog at each sample: the
	// broker queue (tasqueue:tasks) plus the runtime's dispatch buffer —
	// tasks drained from the broker awaiting a poller take (the broker is
	// fully drained into dispatch each pump tick, so the dispatch buffer is
	// where the backlog actually lives).
	MetricQueueDepth = "tasqueue.queue.depth"
	// MetricCompletionLatency is the enqueue→completed latency histogram:
	// one sample per completed job (plain success, chain tail, or group
	// join), measured as completion time − the job's ORIGINAL broker-enqueue
	// time. Retried jobs keep their original enqueue time, so the sample
	// covers the whole lifecycle (retry pile-up included). Chain steps
	// inherit the chain head's enqueue clock (chainStartMs), so a chain
	// tail's sample spans the whole chain, head enqueue → tail completion.
	// Percentile summary via monitor.HistogramSummary (p50/p90/p99/max).
	MetricCompletionLatency = "tasqueue.completion.latency"
)

// noGroup is the groupID sentinel for jobs that are NOT group members
// (plain jobs, chain steps, OnError children). Group accounting keys off
// this sentinel rather than the config knob so non-member jobs can never
// collide with a real group (the first group created has id 0).
const noGroup = ^uint64(0)

// runtime is the shared mediator between the queue (broker) and worker
// services: the real mechanism state each service's Build closure
// constructs, plus the per-pump drive that advances it. Services share one
// Go package and this runtime by design; the System wires the declared
// ports, owns the core.Simulation and the free monitor, and drives rt.tick
// once per pump tick.
//
// Determinism: the runtime is single-goroutine and is only mutated by pump
// tick order and scheduled events; all RNG is via substream draws at fixed
// keys (failRNG lazily created on the first failing attempt; the queue's
// drop substream at SetDropRate); no map iteration on a simulation path
// (jobs/groupRem/groupFailed are indexed lookup only). No goroutines, no
// wall-clock. Root-level substream keys (tasqueue.fail, the queue drop key)
// presume one tasqueue system per simulation — the composition note in
// doc.go.
type runtime struct {
	cfg Config
	sim *core.Simulation

	// Broker (owned by QueueService).
	q     *model.TaskQueue // tasqueue:tasks — the pending-job queue
	timer *model.TaskQueue // ETA/scheduled (EnqueueScheduled abstraction)

	// Workers (owned by WorkerService).
	poller   *model.Poller // the per-queue consumer fleet
	dispatch []model.Task  // drained from q, awaiting a poller take

	// Results.
	stats Stats

	// lat accumulates enqueue→completed latency samples for the automatic
	// histogram; nil unless monitoring is enabled (attachMetrics).
	lat *latencyCollector

	failRNG core.RNG // nil until FailP > 0 and the first failing attempt

	// effFailP is the effective per-attempt handler failure probability:
	// the static FailP baseline, overridden by an active KindFailure fault
	// window (models/tasqueue/faults). complete's RNG decision reads this,
	// so a fault can raise it mid-run and the baseline is restored after
	// the window.
	effFailP float64
	// effTimeoutMs is the effective JobOpts.Timeout deadline: the static
	// TimeoutMs baseline, overridden by an active KindTimeout fault window.
	// take's deadline check reads this, so attempts time out only while a
	// window is active.
	effTimeoutMs int64

	jobs      map[uint64]*jobState
	groupRem  map[uint64]int  // groupID -> remaining members
	groupFail map[uint64]bool // groupID -> any member terminally failed (getGroupStatus)
	nextID    uint64
	groupID   uint64

	// cap is the runtime mechanism recorder (model/capture). It is nil when
	// capture is disabled (the byte-neutral default: every record site is a
	// nil-check). When enabled it is attached on the first tick from the
	// System (which creates it at Build) — see attachCap. The recorder is
	// observational: zero RNG draws, zero I/O inside Run, and never a
	// topology or monitor node.
	cap *capture.Recorder

	endMs   int64
	started bool // first-tick one-time inflow scheduling (start)
}

// newRuntime validates and normalizes cfg and constructs the stateless
// parts of the runtime. The sim and the service-owned primitives are added
// by BuildSystem and the service Build closures.
func newRuntime(cfg Config) (*runtime, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &runtime{
		cfg:          cfg,
		endMs:        cfg.SimEndMs,
		effFailP:     cfg.FailP,
		effTimeoutMs: cfg.TimeoutMs,
		jobs:         map[uint64]*jobState{},
		groupRem:     map[uint64]int{},
		groupFail:    map[uint64]bool{},
	}, nil
}

// jobState is the per-job mechanism state: the DAG role (chain index and
// group membership) and the attempt/retry counters.
type jobState struct {
	id       uint64
	chainIdx int    // 0..ChainLen-1, -1 = no chain
	groupID  uint64 // group id, or noGroup when not a member
	attempt  int32  // 1-based handler attempt
	retried  uint32 // retries already performed (the server's Retried counter)
	// enqueuedMs is the job's ORIGINAL broker-enqueue clock (set on first
	// registration; preserved across retries) — the completion-latency
	// histogram's start edge for plain and group jobs.
	enqueuedMs int64
	// chainStartMs is the chain HEAD's broker-enqueue clock, propagated to
	// every successor: chain-step completions measure latency from the
	// head's enqueue (the whole chain lifecycle), not the step's own.
	// Stamped on the head's first registration; copied by chain
	// continuation (runtime.go complete). Zero for non-chain jobs.
	chainStartMs int64
}

// latencyCollector accumulates enqueue→completed latency samples between
// monitor samples. It is created by attachMetrics ONLY when monitoring is
// enabled; complete appends when it is non-nil, and the monitor's
// AddHistogram closure drains it at every Sample. Purely deterministic:
// zero RNG draws, zero simulation mutation (the sim clock minus the job's
// recorded enqueue clock).
type latencyCollector struct {
	samples []int64
}

// tick is the per-pump drive, registered on the System as a tick hook. It
// runs once per pump tick and advances the broker in the real service
// order: one-time inflow start → timer→q (ETA) → q→dispatch drain with
// drop/expire handling.
func (rt *runtime) tick(now core.SimTime) {
	if now.Int64() >= rt.endMs {
		return
	}
	if !rt.started {
		rt.started = true
		rt.start(now)
	}
	rt.applyFaults(now.Int64())
	// 1. Timer queue → tasqueue:tasks (EnqueueScheduled / ETA jobs). A
	// capacity-full handoff is rejected like any other add (RejectedAdds,
	// terminal for the job — config.go QueueCapacity): the jobState is
	// guaranteed registered here (enqueueScheduled parked it), so the
	// reject funnels through terminal for group accounting too. The timer
	// queue carries no fault knobs (no drop/maxwait/delay are ever
	// applied), so its drain's dropped/expired slices are always empty.
	if due, _, _ := rt.timer.DrainToTick(now); len(due) > 0 {
		for _, task := range due {
			// Capture: load → queue — the job's ETA arrived; it re-enters
			// the broker queue (EnqueueScheduled) — Q3.
			rt.rec("load", "queue", "eta_enqueue", now.Int64(), task)
			if !rt.q.AddAt(task, now) {
				rt.stats.RejectedAdds++
				if js, ok := rt.jobs[task.ID]; ok {
					rt.terminal(js)
				}
			}
		}
	}
	// 2. Broker drain → dispatch. A drain-time drop is modeled as an
	// attempt-level failure — it consumes one retry and tallies
	// Retried/Failed per the budget, but never reaches the handler (the
	// divergence is documented on config.go DropRate). An expired task is
	// terminal (non-retryable, MaxWaitMs deadline) and fails its group if
	// it was a member (getGroupStatus semantics: one failed member fails
	// the group).
	delivered, dropped, expired := rt.q.DrainToTick(now)
	rt.dispatch = append(rt.dispatch, delivered...)
	rt.stats.Dropped += uint64(len(dropped))
	rt.stats.Expired += uint64(len(expired))
	rt.stats.Failed += uint64(len(expired))
	for _, tt := range delivered {
		// Capture: queue → dispatch — the broker delivered a job to the
		// dispatch buffer (awaiting a poller take) — Q4.
		rt.rec("queue", "dispatch", "deliver", now.Int64(), tt)
	}
	for _, tt := range dropped {
		task := tt
		js, ok := rt.jobs[task.ID]
		if !ok {
			continue
		}
		if rt.canRetry(js) {
			rt.stats.Retried++
			js.retried++
			js.attempt++
			task.Attempt = js.attempt
			// Capture: queue → load — a drain-time drop re-enters the broker
			// (retryJob, immediate) — Q4 drop.
			rt.rec("queue", "load", "drop_retry", now.Int64(), task)
			rt.sim.ScheduleAt(now, func(*core.Simulation) { rt.enqueueTask(task, js, rt.sim.Now()) })
		} else {
			rt.stats.Failed++
			// Capture: queue → terminal — the drop exhausted the retry
			// budget — Q4 drop terminal.
			rt.rec("queue", "terminal", "drop_terminal", now.Int64(), task)
			rt.terminal(js)
		}
	}
	for _, tt := range expired {
		js, ok := rt.jobs[tt.ID]
		if !ok {
			continue
		}
		// Capture: queue → terminal — the job expired in the queue
		// (MaxWaitMs deadline) — Q4 expire.
		rt.rec("queue", "terminal", "expire", now.Int64(), tt)
		rt.terminal(js)
	}
}

// terminal removes a job that can no longer make progress: the terminal
// failure paths of the drop loop, the expiry path and the handler-exhausted
// path in complete all funnel here. Group members mark their group failed
// (getGroupStatus — one failed member fails the whole group) and the group
// join counter is decremented; on the last member the group bookkeeping is
// removed.
func (rt *runtime) terminal(js *jobState) {
	delete(rt.jobs, js.id)
	if js.groupID == noGroup {
		return
	}
	gid := js.groupID
	if rem, ok := rt.groupRem[gid]; ok {
		// The group-fail mark lives only while the group is tracked: if
		// the bookkeeping is already gone (last member ended), there is
		// nothing left to fail and no stale entry to resurrect.
		rt.groupFail[gid] = true
		rem--
		if rem <= 0 {
			delete(rt.groupRem, gid)
			delete(rt.groupFail, gid)
		} else {
			rt.groupRem[gid] = rem
		}
	}
}

// start runs once, on the first pump tick, before any task can reach the
// broker: it enqueues the initial burst (immediately — the same tick's
// drain delivers it), starts the worker fleet, and schedules the periodic
// inflow (scheduleEvery / cron).
func (rt *runtime) start(now core.SimTime) {
	rt.burstEnqueue(now)
	rt.poller.Start()
	if rt.cfg.ScheduleEveryMs > 0 {
		var tick func()
		tick = func() {
			if rt.sim.Now().Int64() >= rt.endMs {
				return
			}
			rt.burstEnqueue(rt.sim.Now())
			rt.sim.ScheduleIn(rt.cfg.ScheduleEveryMs, func(*core.Simulation) { tick() })
		}
		rt.sim.ScheduleIn(rt.cfg.ScheduleEveryMs, func(*core.Simulation) { tick() })
	}
	rt.scheduleCron()
}

// canRetry reports whether the job may be retried after the attempt that
// just failed: the server retries while Retried < MaxRetries (retryJob
// increments Retried and re-enqueues immediately — no backoff).
func (rt *runtime) canRetry(js *jobState) bool {
	return js.retried < rt.cfg.MaxRetries
}

// failDraw returns the per-attempt handler failure RNG, creating the
// failP substream on first use. Zero draws when the effective failP is off.
func (rt *runtime) failDraw() float64 {
	if rt.failRNG == nil {
		rt.failRNG = rt.sim.Substream("tasqueue.fail")
	}
	return rt.failRNG.Float64()
}

// burstEnqueue enqueues BurstCount jobs at atMs — as a group fan-out
// (GroupSize > 0), a chain head (ChainLen > 0), or plain jobs — each
// routed through ETAOffsetMs when set.
func (rt *runtime) burstEnqueue(atMs core.SimTime) {
	for i := 0; i < rt.cfg.BurstCount; i++ {
		rt.stats.Started++
		if rt.cfg.GroupSize > 0 {
			gid := rt.groupID
			rt.groupID++
			rt.groupRem[gid] = rt.cfg.GroupSize
			for j := 0; j < rt.cfg.GroupSize; j++ {
				id := rt.nextID
				rt.nextID++
				task := model.Task{ID: id, Attempt: 1}
				js := &jobState{id: id, chainIdx: -1, groupID: gid, attempt: 1}
				rt.enqueueWithETA(task, js, atMs)
			}
			continue
		}
		if rt.cfg.ChainLen > 0 {
			id := rt.nextID
			rt.nextID++
			task := model.Task{ID: id, Attempt: 1}
			js := &jobState{id: id, chainIdx: 0, groupID: noGroup, attempt: 1}
			rt.enqueueWithETA(task, js, atMs)
			continue
		}
		id := rt.nextID
		rt.nextID++
		task := model.Task{ID: id, Attempt: 1}
		js := &jobState{id: id, chainIdx: -1, groupID: noGroup, attempt: 1}
		rt.enqueueWithETA(task, js, atMs)
	}
}

// enqueueWithETA routes a job to the broker: straight into tasqueue:tasks,
// or via the timer queue when ETAOffsetMs > 0 (EnqueueScheduled — the
// broker divergence documented in queue.go).
func (rt *runtime) enqueueWithETA(task model.Task, js *jobState, atMs core.SimTime) {
	if rt.cfg.ETAOffsetMs > 0 {
		rt.enqueueScheduled(task, js, atMs.Add(rt.cfg.ETAOffsetMs))
		return
	}
	rt.enqueueTask(task, js, atMs)
}

// enqueueTask registers the job and adds it to the pending queue at atMs.
// The job's original enqueue clock is recorded on FIRST registration only
// — retries reuse the same jobState, so the completion-latency histogram
// keeps measuring from the original enqueue (total lifecycle latency). A
// chain head's clock is also stamped onto chainStartMs, which chain
// continuation copies to successors (the chain's whole-lifecycle start).
// A capacity-full add is REJECTED and counted (the RejectedAdds boundary):
// the rejected job is removed via terminal — terminal for the job, and for
// a group member it fails the group (a member that can never run is a
// failed member, getGroupStatus). A rejected chain-step add stops the
// chain silently (counted RejectedAdds only — the admission-boundary loss
// documented on config.go QueueCapacity).
func (rt *runtime) enqueueTask(task model.Task, js *jobState, atMs core.SimTime) {
	if _, exists := rt.jobs[task.ID]; !exists {
		js.enqueuedMs = atMs.Int64()
		if js.chainIdx == 0 {
			js.chainStartMs = atMs.Int64()
		}
	}
	rt.jobs[task.ID] = js
	// Capture: load → queue — a job is enqueued into the broker
	// (initial burst, retry re-entry, chain continuation, OnError) — Q1.
	rt.rec("load", "queue", "enqueue", atMs.Int64(), task)
	if !rt.q.AddAt(task, atMs) {
		rt.stats.RejectedAdds++
		rt.terminal(js)
	}
}

// enqueueScheduled registers the job in the timer queue for delivery at
// atMs (ETA / EnqueueScheduled). The first-registration clock stamping is
// shared with enqueueTask (a chain head parked behind an ETA measures its
// latency from the scheduled broker entry, not the burst time).
func (rt *runtime) enqueueScheduled(task model.Task, js *jobState, atMs core.SimTime) {
	if _, exists := rt.jobs[task.ID]; !exists {
		js.enqueuedMs = atMs.Int64()
		if js.chainIdx == 0 {
			js.chainStartMs = atMs.Int64()
		}
	}
	rt.jobs[task.ID] = js
	// Capture: load → queue — a job is scheduled (ETA / EnqueueScheduled)
	// into the timer queue for delivery at atMs — Q2. The record uses the
	// same load → queue edge as enqueueTask because the ETA timer parks the
	// job in the broker's timer queue, not in a distinct mechanism
	// component; the timer-arrival drain (Q3) reuses the same edge.
	rt.rec("load", "queue", "eta_enqueue", atMs.Int64(), task)
	rt.timer.AddAt(task, atMs)
}

// take is the poller's dispatch hook: it pops one dispatched task, computes
// the attempt timeout (ServiceMs > TimeoutMs ⇒ the handler context deadline
// expires — server.go:388-390), and schedules the completion after
// ServiceMs. Returns true when a task was taken (poller in-flight
// accounting).
func (rt *runtime) take(now core.SimTime) bool {
	if len(rt.dispatch) == 0 {
		return false
	}
	task := rt.dispatch[0]
	rt.dispatch = rt.dispatch[1:]
	js, ok := rt.jobs[task.ID]
	if !ok {
		// Defensive only — unreachable by construction: every task in
		// dispatch was delivered by the broker drain, and every delivered
		// task's jobState is live until the task leaves dispatch (terminal
		// and success delete only after take pops it). Returning false (no
		// task taken) keeps the poller's in-flight accounting exact — the
		// popped phantom without a jobState has no completion to release.
		return false
	}
	timeout := rt.effTimeoutMs > 0 && rt.cfg.ServiceMs > rt.effTimeoutMs
	// Capture: dispatch → exec — a poller took the job and will run the
	// handler — Q5.
	rt.rec("dispatch", "exec", "take", now.Int64(), task)
	rt.sim.ScheduleAt(now.Add(rt.cfg.ServiceMs), func(*core.Simulation) {
		defer rt.poller.Release()
		rt.complete(task, js, timeout)
	})
	return true
}

// complete finalizes one handler attempt: a timed-out attempt fails
// without an RNG draw; otherwise the EFFECTIVE fail probability (static
// FailP, or an active KindFailure fault window) decides. A failed attempt
// is retried immediately (retryJob — no backoff) while the budget remains,
// or becomes terminal: OnError jobs are enqueued, the job is failed, and
// the DAG bookkeeping (chain stop / group fail) runs. A successful attempt
// continues the chain (OnSuccess → next job), joins the group, or
// completes plain jobs (server.go:455-474).
func (rt *runtime) complete(task model.Task, js *jobState, timedOut bool) {
	rt.stats.AttemptsTotal++
	failed := timedOut
	if !failed && rt.effFailP > 0 {
		failed = rt.failDraw() < rt.effFailP
	}
	if failed {
		if rt.canRetry(js) {
			task.Attempt = js.attempt + 1
			rt.stats.Retried++
			js.retried++
			js.attempt++
			// Capture: exec → load — the attempt failed and is retried
			// immediately (retryJob, no backoff) — Q6 retry.
			rt.rec("exec", "load", "retry", rt.sim.Now().Int64(), task)
			rt.sim.ScheduleAt(rt.sim.Now(), func(*core.Simulation) { rt.enqueueTask(task, js, rt.sim.Now()) })
			return
		}
		// Terminal failure — enqueue each Job.OnError (server.go:433-444),
		// then statusFailed. OnError children are ordinary jobs: they join
		// no group and start no chain.
		rt.stats.Failed++
		// Capture: exec → terminal — the attempt failed past its retry
		// budget (or timed out) — Q6 fail.
		rt.rec("exec", "terminal", "fail", rt.sim.Now().Int64(), task)
		if rt.cfg.OnErrorJobs > 0 {
			for i := 0; i < rt.cfg.OnErrorJobs; i++ {
				id := rt.nextID
				rt.nextID++
				et := model.Task{ID: id, Attempt: 1}
				ejs := &jobState{id: id, chainIdx: -1, groupID: noGroup, attempt: 1}
				rt.enqueueTask(et, ejs, rt.sim.Now())
				rt.stats.OnErrorSpawned++
			}
		}
		rt.terminal(js)
		if js.chainIdx >= 0 {
			return // chain fails — no further steps
		}
		return
	}
	// Success — chain continuation (OnSuccess → next job; a chain step
	// enqueues its successor — server.go:456). The successor inherits the
	// chain's lifecycle start clock, so the final step's latency spans the
	// whole chain, head enqueue → tail completion.
	if js.chainIdx >= 0 {
		rt.stats.ChainStepsDone++
		delete(rt.jobs, task.ID)
		if js.chainIdx+1 < rt.cfg.ChainLen {
			id := rt.nextID
			rt.nextID++
			nt := model.Task{ID: id, Attempt: 1}
			njs := &jobState{id: id, chainIdx: js.chainIdx + 1, groupID: noGroup, attempt: 1, chainStartMs: js.chainStartMs}
			// Capture: exec → load — the step succeeded and enqueues its
			// chain successor (OnSuccess) — Q6 chain.
			rt.rec("exec", "load", "chain_continue", rt.sim.Now().Int64(), nt)
			rt.enqueueTask(nt, njs, rt.sim.Now())
			return
		}
		rt.stats.Completed++
		rt.recordLatency(js)
		// Capture: exec → terminal — the chain tail completed — Q6 chain
		// terminal.
		rt.rec("exec", "terminal", "chain_complete", rt.sim.Now().Int64(), task)
		return
	}
	// Group member success: the group completes only when the LAST member
	// succeeds AND no member failed (getGroupStatus — Tasqueue/groups.go:
	// 125-137). A failed member marks the group failed (terminal),
	// suppressing the joiner's Completed even if siblings succeed later.
	if js.groupID != noGroup {
		rt.stats.GroupJobsDone++
		gid := js.groupID
		delete(rt.jobs, task.ID)
		// Capture: exec → terminal — a group member succeeded (the group
		// join is decided below) — Q6 group member success.
		rt.rec("exec", "terminal", "group_member_ok", rt.sim.Now().Int64(), task)
		if rem, ok := rt.groupRem[gid]; ok {
			rem--
			if rem <= 0 {
				failed := rt.groupFail[gid]
				delete(rt.groupRem, gid)
				delete(rt.groupFail, gid)
				if !failed {
					rt.stats.Completed++
					rt.recordLatency(js)
				}
			} else {
				rt.groupRem[gid] = rem
			}
		}
		return
	}
	rt.stats.Completed++
	rt.recordLatency(js)
	delete(rt.jobs, task.ID)
	// Capture: exec → terminal — a plain job completed — Q6 plain success.
	rt.rec("exec", "terminal", "complete", rt.sim.Now().Int64(), task)
}

// recordLatency appends one enqueue→completed latency sample when the
// completion histogram is active (monitoring enabled). Pure sim-clock read
// minus the job's lifecycle-start clock — never negative (a completion
// always fires at or after its job's broker enqueue). Chain tails measure
// from the chain HEAD's enqueue (chainStartMs), so the sample spans the
// whole chain lifecycle; plain and group jobs measure from their own
// original enqueue.
func (rt *runtime) recordLatency(js *jobState) {
	if rt.lat != nil {
		start := js.enqueuedMs
		if js.chainIdx >= 0 {
			start = js.chainStartMs
		}
		rt.lat.samples = append(rt.lat.samples, rt.sim.Now().Int64()-start)
	}
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
// Component names are the model's own ("load", "queue", "dispatch",
// "exec"); kind is the transition category; task identity is the job ID.
// rec is a no-op when capture is off (nil recorder — byte-neutral).
func (rt *runtime) rec(from, to, kind string, nowMs int64, task model.Task) {
	if rt.cap == nil {
		return
	}
	rt.cap.Record(from, to, kind, nowMs, capture.WithTask(strconv.FormatUint(task.ID, 10), int(task.Attempt), 0))
}

// scheduleCron drives the deterministic cron: any non-empty CronSchedule
// is treated as a fixed 200ms period burst (faithful cron parsing would
// need robfig/cron — out of scope; a fixed interval keeps the model
// deterministic).
func (rt *runtime) scheduleCron() {
	if rt.cfg.CronSchedule == "" {
		return
	}
	const period int64 = 200
	var cronTick func()
	cronTick = func() {
		if rt.sim.Now().Int64() >= rt.endMs {
			return
		}
		rt.burstEnqueue(rt.sim.Now())
		rt.sim.ScheduleIn(period, func(*core.Simulation) { cronTick() })
	}
	rt.sim.ScheduleIn(period, func(*core.Simulation) { cronTick() })
}

// attachMetrics registers the automatic free-monitor metrics on the
// System's monitor: the REAL backlog gauge (broker queue + dispatch buffer),
// the outcome counters, and the enqueue→completed latency histogram. The
// closures are pure reads of runtime state — zero RNG draws, zero
// mutation of simulation state — so the monitor stays determinism-neutral
// (on/off runs are bit-identical; the histogram collector drains at Sample
// and never feeds back). No-op when monitoring is disabled. Called once, on
// the first pump tick (the monitor exists only after Build; the first Sample
// happens before this, so the metrics join from the second tick onward —
// deterministic).
func attachMetrics(sys *model.System, rt *runtime) {
	m := sys.Monitor()
	if m == nil {
		return
	}
	m.AddCounter(MetricStarted, func() uint64 { return rt.stats.Started })
	m.AddCounter(MetricCompleted, func() uint64 { return rt.stats.Completed })
	m.AddCounter(MetricRetried, func() uint64 { return rt.stats.Retried })
	m.AddCounter(MetricFailed, func() uint64 { return rt.stats.Failed })
	m.AddCounter(MetricDropped, func() uint64 { return rt.stats.Dropped })
	m.AddCounter(MetricExpired, func() uint64 { return rt.stats.Expired })
	// The REAL backlog: the broker queue plus the dispatch buffer (tasks
	// drained from the broker, awaiting a poller take). The broker is
	// fully drained into dispatch each pump tick, so this sum — not
	// q.Len() alone — is the honest pending-job depth.
	m.AddGauge(MetricQueueDepth, func() int64 { return int64(rt.q.Len()) + int64(len(rt.dispatch)) })
	// Enqueue→completed latency: the collector is drained at every Sample.
	rt.lat = &latencyCollector{}
	m.AddHistogram(MetricCompletionLatency, func() []int64 {
		batch := rt.lat.samples
		rt.lat.samples = nil
		return batch
	})
	// Opt-in labeled/attributed surfaces (ADR-115). Off by default: nothing
	// new is registered and the determinism digest does not move.
	if rt.cfg.LabeledMetrics {
		rt.attachLabeledMetrics(m)
	}
}

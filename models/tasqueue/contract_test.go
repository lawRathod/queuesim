package tasqueue

import (
	"testing"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/model"
)

// Mechanism-contract tests: the model's constants and behaviors must match
// the REAL Tasqueue server (github.com/lawRathod/tasqueue — the
// `Tasqueue/` submodule at the repo root), which is the source of truth
// (ADR-078 / ADR-093 mapping policy). Each pin carries the submodule
// file:line reference; the facts below were verified against the submodule
// 2026-08-26:
//
//   - DefaultQueue "tasqueue:tasks" Tasqueue/jobs.go:15-18
//   - JobOpts fields: ID, ETA, Queue, MaxRetries, Schedule, Timeout
//     Tasqueue/jobs.go:35-44
//   - NO backoff: retryJob increments Retried and re-enqueues immediately
//     (the broker.Enqueue inside retryJob is the next step — no sleep)
//     Tasqueue/server.go:485-510
//   - Timeout = context deadline now+JobOpts.Timeout; the handler select
//     fires on jctx.Done() — an attempt times out iff ServiceMs > TimeoutMs
//     Tasqueue/server.go:388-390 (deadline), 404-410 (select)
//   - OnError jobs are enqueued once retries are exhausted, then the job is
//     statusFailed
//     Tasqueue/server.go:433-444 (OnError enqueue), 447 (statusFailed)
//   - OnSuccess chain continuation: each successful step enqueues the next
//     (with PrevJobResult fetch)
//     Tasqueue/server.go:455-474
//   - Per-queue concurrency budget (TaskOpts.Concurrency) bounds in-flight
//     handler work
//     Tasqueue/server.go:74-81

// Server-default pins (ms/names) — the file:line refs above are the pins.
// A bare JobOpts{} is the server's ACTUAL default, so the DECLARED and the
// EFFECTIVE retry budgets differ (WP-A1 spike evidence:
// .agents/experiments/2026-09-06-tasqueue-effective-default-spike.md):
//
//   - serverDeclaredMaxRetry (defaultMaxRetry = 1, jobs.go:17) is DEAD CODE:
//     NewJob (jobs.go:80-90) and DefaultMeta (jobs.go:64-76) never read it;
//     DefaultMeta copies opts.MaxRetries (zero for a bare JobOpts{}) straight
//     into Meta.MaxRetry (jobs.go:72).
//   - serverEffectiveMaxRetry (0) is what a bare JobOpts{} actually yields:
//     server.go:423 `if msg.MaxRetry != msg.Retried` is FALSE at 0==0, so the
//     job skips retryJob (:427) and statusFailed (:447) — the first handler
//     error is TERMINAL.
//
// DefaultConfig() pins the EFFECTIVE budget (serverEffectiveMaxRetry), not
// the dead declared constant; callers wanting retries must set MaxRetries
// explicitly (the submodule's own examples pass bare JobOpts{} and therefore
// run with zero retries — only its tests pass MaxRetries:1,
// jobs_test.go:153,201,280; see the A1 memo).
const (
	// serverDefaultQueue is DefaultQueue (Tasqueue/jobs.go:16).
	serverDefaultQueue = "tasqueue:tasks"
	// serverDeclaredMaxRetry is defaultMaxRetry (Tasqueue/jobs.go:17) —
	// declared but NEVER applied by NewJob/DefaultMeta (dead code).
	serverDeclaredMaxRetry uint32 = 1
	// serverEffectiveMaxRetry is what a bare JobOpts{} yields: MaxRetry=0,
	// so the first handler error is terminal (server.go:423,447).
	serverEffectiveMaxRetry uint32 = 0
)

// contractRun runs cfg through the real Topology entry point.
func contractRun(t *testing.T, cfg Config) Result {
	t.Helper()
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	return top.Run()
}

// contractRuntime wires a runtime the way build() does (sim, broker queue,
// timer queue, poller bound to rt.take) so mechanism-unit tests can drive
// the real take/complete path step-by-step and assert sim-time facts that
// the run-level Result surface cannot expose (e.g. "re-enqueued at the same
// sim instant").
func contractRuntime(t *testing.T, cfg Config) (*runtime, *core.Simulation) {
	t.Helper()
	rt, err := newRuntime(cfg)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	sim, err := core.NewSimulation(cfg.Seed, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	rt.sim = sim
	// Same construction as queueService/workerService (queue.go:35,
	// worker.go:26-31) — the broker queue carries DefaultQueue's name.
	rt.q = model.NewTaskQueue(sim, model.TaskQueueConfig{Name: serverDefaultQueue, Capacity: cfg.QueueCapacity})
	rt.timer = model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "tasqueue:timer"})
	rt.poller = model.NewPoller(sim, model.PollerConfig{
		Name:           "tasqueue",
		PollIntervalMs: cfg.PollMs,
		InFlightBudget: cfg.Concurrency * cfg.NumWorkers,
		EndMs:          cfg.SimEndMs,
	}, rt.take)
	return rt, sim
}

// TestDefaultQueueMatchesSubmodule pins the broker queue's name to
// DefaultQueue "tasqueue:tasks" (Tasqueue/jobs.go:16). The model hardcodes
// the name at the queueService construction site (queue.go:35).
func TestDefaultQueueMatchesSubmodule(t *testing.T) {
	top, err := NewTopology(DefaultConfig())
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	sys := top.System()
	// Service Build closures (queueService) construct rt.q; explicit Build
	// materializes it without running the simulation.
	if err := sys.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	// jobs.go:16 — DefaultQueue = "tasqueue:tasks".
	if got := top.rt.q.Name(); got != serverDefaultQueue {
		t.Fatalf("broker queue name = %q, want %q (Tasqueue/jobs.go:16)", got, serverDefaultQueue)
	}
}

// TestDefaultMaxRetriesMatchesSubmodule pins DefaultConfig's retry budget
// to the server's EFFECTIVE default: a bare JobOpts{} carries MaxRetry=0,
// so the first handler failure is TERMINAL (WP-A1 spike — the declared
// defaultMaxRetry = 1 at Tasqueue/jobs.go:17 is dead code; NewJob and
// DefaultMeta never apply it). The semantic consequence (a persistently
// failing job attempts exactly MaxRetries+1 = 1 time) is asserted in
// TestPersistentFailureAttemptsExhaustBudget.
func TestDefaultMaxRetriesMatchesSubmodule(t *testing.T) {
	// The dead declared constant is kept referenced for documentation
	// (serverDeclaredMaxRetry, Tasqueue/jobs.go:17) but is NOT the default.
	if serverDeclaredMaxRetry != 1 {
		t.Fatalf("serverDeclaredMaxRetry = %d, want 1 (Tasqueue/jobs.go:17)", serverDeclaredMaxRetry)
	}
	if got := DefaultConfig().MaxRetries; got != serverEffectiveMaxRetry {
		t.Fatalf("DefaultConfig().MaxRetries = %d, want %d (effective default: bare JobOpts{} → MaxRetry 0, first error terminal, server.go:423,447)", got, serverEffectiveMaxRetry)
	}
}

// TestDefaultConcurrencyPin pins DefaultConfig's per-queue worker budget to
// 1 (model/config.go DefaultConfig — machine-independent determinism;
// the server default is GOMAXPROCS, server.go:91-93). Confirm cells must
// pin Concurrency 1 explicitly (fail-fast): the bubble envelope asserts
// the same single in-flight budget, so model and bubble compare the same
// closure. The budget's MECHANISM (InFlightBudget = Concurrency ×
// NumWorkers) is pinned by TestSlowWorkerQueueBuildUp.
func TestDefaultConcurrencyPin(t *testing.T) {
	if got := DefaultConfig().Concurrency; got != 1 {
		t.Fatalf("DefaultConfig().Concurrency = %d, want 1 (Task 1.1: confirm cells pin Concurrency 1)", got)
	}
}

// TestPersistentFailureAttemptsExhaustBudget is the retry-budget contract:
// MaxRetries counts retries AFTER the initial attempt (server.go:423
// `if msg.MaxRetry != msg.Retried`), so a persistently failing job attempts
// exactly MaxRetries+1 times and then terminally fails via statusFailed
// (server.go:428-447). FailP = 1.0 makes every attempt fail deterministically
// (Float64 ∈ [0,1)), so the counts are exact, not distribution-sensitive.
func TestPersistentFailureAttemptsExhaustBudget(t *testing.T) {
	const burst = 20
	cases := []struct {
		name        string
		maxRetries  uint32
		wantAttempt uint64 // per-job handler attempts = MaxRetries+1
		wantRetried uint64 // per-job re-enqueues = MaxRetries
		wantFailed  uint64 // per-job terminal failures = 1
	}{
		{"maxRetries=0 → 1 attempt", 0, 1, 0, 1},
		// server effective default: bare JobOpts{} → MaxRetry=0 → 1 attempt.
		{"effective default maxRetry=0 → 1 attempt", serverEffectiveMaxRetry, 1, 0, 1},
		// server DECLARED default (dead code, jobs.go:17): if it were applied
		// it would give 2 attempts — the case documents the divergence.
		{"declared maxRetry=1 → 2 attempts", serverDeclaredMaxRetry, 2, 1, 1},
		{"maxRetries=3 → 4 attempts", 3, 4, 3, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Seed = 1
			cfg.BurstCount = burst
			cfg.SimEndMs = 10000
			cfg.ServiceMs = 5
			cfg.FailP = 1.0
			cfg.MaxRetries = tc.maxRetries
			cfg.NumWorkers = 3
			cfg.Concurrency = 1
			cfg.PollMs = 10
			res := contractRun(t, cfg)
			wantAttempts := uint64(burst) * tc.wantAttempt
			wantRetried := uint64(burst) * tc.wantRetried
			if res.Stats.AttemptsTotal != wantAttempts {
				// server.go:485-510: retryJob re-enqueues; budget is
				// Retried < MaxRetries (runtime.canRetry).
				t.Fatalf("AttemptsTotal = %d, want %d (MaxRetries+1 attempts per persistent job)", res.Stats.AttemptsTotal, wantAttempts)
			}
			if res.Stats.Retried != wantRetried {
				t.Fatalf("Retried = %d, want %d (one retry per failing attempt while Retried < MaxRetries)", res.Stats.Retried, wantRetried)
			}
			if res.Stats.Failed != uint64(burst) {
				// server.go:447: statusFailed once the budget is exhausted.
				t.Fatalf("Failed = %d, want %d (every persistent job terminally failed)", res.Stats.Failed, uint64(burst))
			}
			if res.Stats.Completed != 0 {
				t.Fatalf("Completed = %d, want 0 (persistent failure never succeeds)", res.Stats.Completed)
			}
			if aps := res.Stats.AttemptsPerStart(); aps != float64(tc.wantAttempt) {
				t.Fatalf("AttemptsPerStart = %.3f, want %.3f", aps, float64(tc.wantAttempt))
			}
		})
	}
}

// TestRetryReenqueuesAtSameSimTime is the no-backoff pin: retryJob does not
// sleep — it increments Retried and calls broker.Enqueue immediately
// (server.go:485-510) — so the model's retry re-enters the broker at the
// SAME sim time as the failed attempt's completion. Driving the real
// take/complete path event-by-event proves the retry-enqueue event fires at
// Now()==completion time (no +backoff gap), and that the second attempt
// completes exactly ServiceMs after it is taken.
func TestRetryReenqueuesAtSameSimTime(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0 // manual inflow; no periodic traffic
	cfg.Seed = 1
	cfg.SimEndMs = 100000 // keep the poller alive past the assertions
	cfg.ServiceMs = 100
	cfg.FailP = 1.0
	cfg.MaxRetries = 1
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 1000 // one poll per 1000ms: no take in (0,1000) ∪ (1000,2000)
	rt, sim := contractRuntime(t, cfg)

	// One persistently failing job at t=0.
	rt.enqueueTask(model.Task{ID: 1, Attempt: 1}, &jobState{id: 1, chainIdx: -1, groupID: noGroup, attempt: 1}, sim.Now())
	rt.tick(sim.Now()) // drain q→dispatch, start the poller

	// Poll@0 takes the job → completion scheduled at 0+100.
	if !sim.Step() {
		t.Fatal("expected poll event at t=0")
	}
	// Completion@100: the attempt fails; retryJob schedules the re-enqueue.
	if !sim.Step() {
		t.Fatal("expected attempt-1 completion at t=100")
	}
	// server.go:492 — Retried += 1 on the failed attempt.
	if rt.stats.Retried != 1 {
		t.Fatalf("Retried = %d, want 1 after the failing attempt (server.go:492)", rt.stats.Retried)
	}
	// server.go:504 — broker.Enqueue happens immediately, no backoff sleep:
	// the re-enqueue event fires at the SAME sim time (100), not 100+B.
	if !sim.Step() {
		t.Fatal("expected retry re-enqueue event")
	}
	if got := sim.Now().Int64(); got != 100 {
		t.Fatalf("retry re-enqueued at t=%d, want t=100 — a backoff would delay the re-enqueue (server.go:485-510)", got)
	}
	if rt.q.Len() != 1 {
		t.Fatalf("retry not in the broker at the completion instant: q.Len = %d, want 1", rt.q.Len())
	}
	rt.tick(sim.Now()) // drain the retry into dispatch
	if !sim.Step() {
		t.Fatal("expected poll event at t=1000")
	}
	// Attempt 2 completes at take(1000)+ServiceMs(100) = 1100: exactly
	// ServiceMs after the re-enqueue — no backoff delay in the retry path.
	if !sim.Step() {
		t.Fatal("expected attempt-2 completion at t=1100")
	}
	if got := sim.Now().Int64(); got != 1100 {
		t.Fatalf("attempt-2 completion at t=%d, want t=1100 = 1000+ServiceMs (no backoff)", got)
	}
	// Budget exhausted → terminal (server.go:428-447): exactly 2 attempts.
	if rt.stats.Failed != 1 || rt.stats.AttemptsTotal != 2 {
		t.Fatalf("after attempt 2: Failed=%d AttemptsTotal=%d, want 1 and 2 (MaxRetries=1 → 2 attempts)", rt.stats.Failed, rt.stats.AttemptsTotal)
	}
	if rt.q.Len() != 0 {
		t.Fatalf("terminal failure left the job in the broker: q.Len = %d", rt.q.Len())
	}
}

// TestTimeoutSemantics pins JobOpts.Timeout: the handler context deadline is
// set to now+Timeout (server.go:388-390) and the select fires on jctx.Done()
// (server.go:404-410), so an attempt times out iff ServiceMs > TimeoutMs.
// With FailP=0 the failures below can only come from the deadline, and the
// timeout still consumes the retry budget (server.go:423) — exactly 2
// attempts with the explicit MaxRetries=1.
func TestTimeoutSemantics(t *testing.T) {
	const burst = 10
	cases := []struct {
		name        string
		timeoutMs   int64
		wantFailed  uint64 // per-job terminal failures
		wantRetried uint64 // per-job retries (timeouts consume the budget)
		wantDone    uint64 // per-job completions
	}{
		// ServiceMs 100 > TimeoutMs 50 → every attempt times out; the
		// retry budget still applies (2 attempts, then statusFailed).
		{"timeout < service → attempt times out", 50, 1, 1, 0},
		// ServiceMs 100 <= TimeoutMs 200 → no deadline expiry; FailP=0
		// means no failure at all.
		{"timeout >= service → attempt succeeds", 200, 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Seed = 1
			cfg.BurstCount = burst
			cfg.SimEndMs = 10000
			cfg.ServiceMs = 100
			cfg.TimeoutMs = tc.timeoutMs
			cfg.FailP = 0 // failures must come from the timeout, not RNG
			cfg.MaxRetries = 1
			cfg.NumWorkers = 2
			cfg.Concurrency = 1
			cfg.PollMs = 10
			res := contractRun(t, cfg)
			if res.Stats.Failed != uint64(burst)*tc.wantFailed {
				// server.go:405-410 — jctx.Done() expired the attempt.
				t.Fatalf("Failed = %d, want %d (attempt times out iff ServiceMs > TimeoutMs)", res.Stats.Failed, uint64(burst)*tc.wantFailed)
			}
			if res.Stats.Retried != uint64(burst)*tc.wantRetried {
				t.Fatalf("Retried = %d, want %d (timeout consumes the retry budget, server.go:423)", res.Stats.Retried, uint64(burst)*tc.wantRetried)
			}
			if res.Stats.AttemptsTotal != uint64(burst)*(tc.wantRetried+1) {
				t.Fatalf("AttemptsTotal = %d, want %d", res.Stats.AttemptsTotal, uint64(burst)*(tc.wantRetried+1))
			}
			if res.Stats.Completed != uint64(burst)*tc.wantDone {
				t.Fatalf("Completed = %d, want %d", res.Stats.Completed, uint64(burst)*tc.wantDone)
			}
		})
	}
}

// TestTimeoutBoundaryEquality pins the STRICT deadline boundary on the
// static (non-fault) path — the Task 1.2 pin: with ServiceMs == TimeoutMs
// the attempt succeeds (Failed == 0), because take times out iff
// ServiceMs > TimeoutMs (runtime.go take; Tasqueue/server.go:388
// execJob deadline). The fault-window twin
// (TestFaultTimeoutBoundaryNotActiveWhenEqual) covers the effTimeoutMs
// seam; this one covers the TimeoutMs baseline. The equality race is a
// bubble-envelope item: the real handler/deadline select may resolve
// either way when both arms are ready simultaneously.
func TestTimeoutBoundaryEquality(t *testing.T) {
	const burst = 10
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = burst
	cfg.SimEndMs = 10000
	cfg.ServiceMs = 100
	cfg.TimeoutMs = 100 // == ServiceMs: strict > does NOT time out
	cfg.FailP = 0       // failures could only come from the deadline
	cfg.MaxRetries = 1
	cfg.NumWorkers = 2
	cfg.Concurrency = 1
	cfg.PollMs = 10
	res := contractRun(t, cfg)
	if res.Stats.Failed != 0 {
		t.Fatalf("Failed = %d, want 0 (ServiceMs == TimeoutMs must NOT time out — strict > boundary, server.go:388)", res.Stats.Failed)
	}
	if res.Stats.Completed != burst {
		t.Fatalf("Completed = %d, want %d (every attempt succeeds at the boundary)", res.Stats.Completed, burst)
	}
	if res.Stats.AttemptsTotal != burst {
		t.Fatalf("AttemptsTotal = %d, want %d (one clean attempt per job — no retry churn)", res.Stats.AttemptsTotal, burst)
	}
}

// TestTerminalFailureEnqueuesOnErrorJobs pins the OnError contract: once
// retries are exhausted the server enqueues every Job.OnError job, then
// statusFailed (server.go:433-444, 447). Because the model applies the same
// uniform FailP to OnError children (a faithful consequence of them being
// ordinary jobs), the cascade keeps going until the horizon — but at every
// instant the invariant OnErrorSpawned == OnErrorJobs * Failed holds (each
// terminal failure atomically increments Failed and spawns exactly
// OnErrorJobs jobs). The OnErrorJobs=0 control proves the machinery is off
// when unconfigured.
func TestTerminalFailureEnqueuesOnErrorJobs(t *testing.T) {
	const burst = 4
	run := func(onErrorJobs int) Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = burst
		cfg.SimEndMs = 5000
		cfg.ServiceMs = 5
		cfg.FailP = 1.0
		cfg.MaxRetries = 1
		cfg.OnErrorJobs = onErrorJobs
		cfg.NumWorkers = 3
		cfg.Concurrency = 1
		cfg.PollMs = 10
		return contractRun(t, cfg)
	}

	res := run(2)
	// server.go:433-444 — every terminal failure enqueues OnErrorJobs jobs.
	if res.Stats.OnErrorSpawned != uint64(2)*res.Stats.Failed {
		t.Fatalf("OnErrorSpawned = %d, want %d = OnErrorJobs(%d) × Failed(%d) (server.go:433-444)",
			res.Stats.OnErrorSpawned, uint64(2)*res.Stats.Failed, 2, res.Stats.Failed)
	}
	// Every parent job terminally failed, so its OnError spawns happened.
	if res.Stats.Failed < uint64(burst) {
		t.Fatalf("Failed = %d, want >= %d (all parent jobs terminally failed)", res.Stats.Failed, uint64(burst))
	}
	if res.Stats.OnErrorSpawned < uint64(burst*2) {
		t.Fatalf("OnErrorSpawned = %d, want >= %d (each parent's terminal failure spawned its OnError jobs)", res.Stats.OnErrorSpawned, uint64(burst*2))
	}

	// Control: no OnError list → nothing spawned; parents still failed.
	ctrl := run(0)
	if ctrl.Stats.OnErrorSpawned != 0 {
		t.Fatalf("OnErrorJobs=0: OnErrorSpawned = %d, want 0", ctrl.Stats.OnErrorSpawned)
	}
	if ctrl.Stats.Failed != uint64(burst) {
		t.Fatalf("OnErrorJobs=0: Failed = %d, want %d", ctrl.Stats.Failed, uint64(burst))
	}
}

// TestChainSuccessContinuation pins the OnSuccess chain contract: each
// successful step enqueues the next job (server.go:455-474), so with
// ChainLen=3 every burst job performs exactly 3 handler attempts, records 3
// chain steps, and only the FINAL step increments Completed. In-order
// same-instant continuation is asserted separately at the event level
// (TestChainSuccessorEnqueuedAtSameSimTime).
func TestChainSuccessContinuation(t *testing.T) {
	const burst = 10
	const chainLen = 3
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = burst
	cfg.SimEndMs = 10000
	cfg.ServiceMs = 5
	cfg.FailP = 0
	cfg.ChainLen = chainLen
	cfg.NumWorkers = 3
	cfg.Concurrency = 1
	cfg.PollMs = 10
	res := contractRun(t, cfg)
	wantSteps := uint64(burst * chainLen)
	// server.go:456-473 — each non-final success enqueues its successor.
	if res.Stats.ChainStepsDone != wantSteps {
		t.Fatalf("ChainStepsDone = %d, want %d = BurstCount×ChainLen (chain.go OnSuccess link, server.go:455-474)", res.Stats.ChainStepsDone, wantSteps)
	}
	// server.go:476-479 — only the final step's success completes the job.
	if res.Stats.Completed != uint64(burst) {
		t.Fatalf("Completed = %d, want %d (only the final chain step counts)", res.Stats.Completed, uint64(burst))
	}
	if res.Stats.AttemptsTotal != wantSteps {
		t.Fatalf("AttemptsTotal = %d, want %d (one clean attempt per chain step)", res.Stats.AttemptsTotal, wantSteps)
	}
	if res.Stats.Retried != 0 || res.Stats.Failed != 0 {
		t.Fatalf("clean chain: Retried=%d Failed=%d, want 0", res.Stats.Retried, res.Stats.Failed)
	}
}

// TestChainSuccessorEnqueuedAtSameSimTime is the event-level chain-order
// pin: a step's success enqueues its successor at the SAME sim instant
// (server.go:456 — enqueueWithMeta directly in the success path), and the
// steps of one chain run strictly in order (step i+1 exists only after step
// i's success). Driving the real take/complete path proves the timing and
// the final-chain accounting.
func TestChainSuccessorEnqueuedAtSameSimTime(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 100
	cfg.FailP = 0
	cfg.ChainLen = 3
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 1000 // polls at 0, 1000, 2000: no take between steps
	rt, sim := contractRuntime(t, cfg)

	// Chain head (step 0) at t=0.
	rt.enqueueTask(model.Task{ID: 1, Attempt: 1}, &jobState{id: 1, chainIdx: 0, groupID: noGroup, attempt: 1}, sim.Now())
	rt.tick(sim.Now())

	// Step 0: taken poll@0, completes t=100 → success → successor enqueued.
	if !sim.Step() {
		t.Fatal("expected poll event at t=0")
	}
	if !sim.Step() {
		t.Fatal("expected step-0 completion at t=100")
	}
	if rt.stats.ChainStepsDone != 1 {
		t.Fatalf("ChainStepsDone = %d, want 1 after step 0", rt.stats.ChainStepsDone)
	}
	// server.go:456-473 — the successor enters the broker at the SAME sim
	// time as the step's success (enqueueTask runs inside the completion
	// event at Now()==100, not at +ServiceMs).
	if got := sim.Now().Int64(); got != 100 {
		t.Fatalf("step-1 enqueued at t=%d, want t=100 (same-instant OnSuccess continuation, server.go:455-474)", got)
	}
	if rt.q.Len() != 1 {
		t.Fatalf("step-1 not in the broker: q.Len = %d, want 1", rt.q.Len())
	}
	if rt.stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (step 0 is not the final step)", rt.stats.Completed)
	}
	rt.tick(sim.Now())

	// Step 1: taken poll@1000, completes t=1100 → successor enqueued.
	if !sim.Step() {
		t.Fatal("expected poll event at t=1000")
	}
	if !sim.Step() {
		t.Fatal("expected step-1 completion at t=1100")
	}
	if rt.stats.ChainStepsDone != 2 {
		t.Fatalf("ChainStepsDone = %d, want 2 after step 1", rt.stats.ChainStepsDone)
	}
	if got := sim.Now().Int64(); got != 1100 {
		t.Fatalf("step-2 enqueued at t=%d, want t=1100", got)
	}
	if rt.q.Len() != 1 {
		t.Fatalf("step-2 not in the broker: q.Len = %d, want 1", rt.q.Len())
	}
	rt.tick(sim.Now())

	// Step 2 (final): taken poll@2000, completes t=2100 → Completed=1.
	if !sim.Step() {
		t.Fatal("expected poll event at t=2000")
	}
	if !sim.Step() {
		t.Fatal("expected step-2 completion at t=2100")
	}
	if rt.stats.Completed != 1 {
		// server.go:476-479 — the job completes only at the chain tail.
		t.Fatalf("Completed = %d, want 1 at the final step", rt.stats.Completed)
	}
	if rt.stats.ChainStepsDone != uint64(cfg.ChainLen) {
		t.Fatalf("ChainStepsDone = %d, want %d (all steps of the chain)", rt.stats.ChainStepsDone, uint64(cfg.ChainLen))
	}
	if rt.stats.AttemptsTotal != uint64(cfg.ChainLen) {
		t.Fatalf("AttemptsTotal = %d, want %d", rt.stats.AttemptsTotal, uint64(cfg.ChainLen))
	}
	if rt.q.Len() != 0 {
		t.Fatalf("final step left a successor in the broker: q.Len = %d", rt.q.Len())
	}
}

// TestSlowWorkerQueueBuildUp pins the concurrency-budget contract
// (TaskOpts.Concurrency bounds in-flight work per queue — server.go:74-81;
// worker.go:29 InFlightBudget = Concurrency × NumWorkers): with a slow
// handler (ServiceMs=50) and a single-unit budget, a 100-job burst cannot
// drain within the horizon, so Completed < BurstCount — the unconsumed
// backlog is the model's Stats-level buildup observable. MetricQueueDepth
// now samples the REAL backlog (broker queue + dispatch buffer —
// runtime.go gauge closure), so the gauge's final sample equals the
// Stats-equivalent backlog exactly: every un-completed task sits in the
// dispatch buffer awaiting a poller take (zero retries here — FailP=0,
// MaxRetries=0 — so nothing is transient).
func TestSlowWorkerQueueBuildUp(t *testing.T) {
	const burst = 100
	const horizon = 3000
	run := func(serviceMs int64, workers, concurrency int) Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = burst
		cfg.SimEndMs = horizon
		cfg.ServiceMs = serviceMs
		cfg.NumWorkers = workers
		cfg.Concurrency = concurrency
		cfg.PollMs = 1
		cfg.FailP = 0
		cfg.MaxRetries = 0
		return contractRun(t, cfg)
	}

	slow := run(50, 1, 1) // 1 job per 50ms → ~60 of 100 within 3000ms
	fast := run(1, 3, 3)  // 3 jobs per 1ms → all 100 within 3000ms

	slowDepth, slowOK := slow.Metrics.Gauge(MetricQueueDepth)
	fastDepth, fastOK := fast.Metrics.Gauge(MetricQueueDepth)
	// The depth gauge is registered and sampled every pump tick (the metric
	// surface exists), and it now reads the REAL backlog (broker queue +
	// dispatch buffer, runtime.go gauge closure) — so it rises with the
	// slow-worker buildup instead of staying at 0.
	if !slowOK || !fastOK || slowDepth.Count == 0 || fastDepth.Count == 0 {
		t.Fatalf("MetricQueueDepth not sampled: slow(ok=%v count=%d) fast(ok=%v count=%d)",
			slowOK, slowDepth.Count, fastOK, fastDepth.Count)
	}

	// The concurrency budget (server.go:74-81) serializes the slow handler:
	// only ~60 of 100 jobs complete → a 40-job backlog at the horizon.
	if slow.Stats.Completed >= uint64(burst) {
		t.Fatalf("slow worker completed %d/%d jobs, want < %d (buildup: handler at 50ms cannot drain 100 jobs in %dms)", slow.Stats.Completed, burst, burst, horizon)
	}
	// Fast handler drains the same burst completely.
	if fast.Stats.Completed != uint64(burst) {
		t.Fatalf("fast worker completed %d/%d jobs, want %d", fast.Stats.Completed, burst, burst)
	}
	slowBacklog := uint64(burst) - slow.Stats.Completed
	if fastBacklog := uint64(burst) - fast.Stats.Completed; fastBacklog != 0 {
		t.Fatalf("fast backlog = %d, want 0", fastBacklog)
	}
	if slowBacklog == 0 {
		t.Fatal("slow backlog = 0, want > 0")
	}
	// The gauge's FINAL sample (System.Run final monitor sample, after the
	// tail events) sees the true backlog: under zero retries every
	// un-completed task still sits in the dispatch buffer, so the last
	// gauge value equals BurstCount − Completed exactly. The fast run drains
	// everything, so its final gauge is 0.
	if slowGauge := slowDepth.Last; uint64(slowGauge) != slowBacklog {
		t.Fatalf("slow depth gauge last=%d, want %d (backlog lives in the dispatch buffer, now sampled)", slowGauge, slowBacklog)
	}
	if fastDepth.Last != 0 {
		t.Fatalf("fast depth gauge last=%d, want 0 (fast handler drained the burst)", fastDepth.Last)
	}
	t.Logf("slow: completed=%d backlog=%d depthLast=%d depthMax=%d | fast: completed=%d backlog=0 depthLast=%d depthMax=%d",
		slow.Stats.Completed, slowBacklog, slowDepth.Last, slowDepth.Max, fast.Stats.Completed, fastDepth.Last, fastDepth.Max)
}

// TestRunLevelStormAmplification is the run-level amplification contract:
// the same seed/model with an elevated handler failure rate produces
// clearly measurable retry amplification — attempts/start, retried and
// failed all sit well above the calm baseline. Calm FailP=0.1 mostly
// succeeds; storm FailP=0.7 exhausts the default retry budget on ~40% of
// jobs.
func TestRunLevelStormAmplification(t *testing.T) {
	const seed = int64(42)
	base := DefaultConfig()
	base.Seed = seed
	base.BurstCount = 100
	base.SimEndMs = 10000
	base.ServiceMs = 5
	base.MaxRetries = 1
	base.NumWorkers = 3
	base.Concurrency = 1
	base.PollMs = 10

	storm := base
	storm.FailP = 0.7
	rs := contractRun(t, storm)
	calm := base
	calm.FailP = 0.1
	rc := contractRun(t, calm)

	stormAps := rs.Stats.AttemptsPerStart()
	calmAps := rc.Stats.AttemptsPerStart()
	t.Logf("seed %d storm: aps=%.3f retried=%d failed=%d completed=%d", seed, stormAps, rs.Stats.Retried, rs.Stats.Failed, rs.Stats.Completed)
	t.Logf("seed %d calm : aps=%.3f retried=%d failed=%d completed=%d", seed, calmAps, rc.Stats.Retried, rc.Stats.Failed, rc.Stats.Completed)

	// Storm retries clearly above calm (failP 0.7 vs 0.1, same seed):
	// every failed attempt re-enqueues immediately (server.go:485-510).
	if rs.Stats.Retried <= rc.Stats.Retried*2 {
		t.Fatalf("storm retried=%d not clearly above calm retried=%d (want > 2×; retryJob immediate re-enqueue, server.go:485-510)", rs.Stats.Retried, rc.Stats.Retried)
	}
	if stormAps <= calmAps+0.25 {
		t.Fatalf("storm aps=%.3f not clearly above calm aps=%.3f (want > +0.25)", stormAps, calmAps)
	}
	if rs.Stats.Failed <= rc.Stats.Failed+20 {
		t.Fatalf("storm failed=%d not clearly above calm failed=%d (want > +20)", rs.Stats.Failed, rc.Stats.Failed)
	}
}

// TestGroupAllSuccessCompletesOnce pins the group join contract: with
// every member succeeding, each GROUP completes exactly once — Completed
// counts groups, GroupJobsDone counts members, and the per-member attempt
// rate is one. Started counts one per group burst item (the convention
// documented on Stats).
func TestGroupAllSuccessCompletesOnce(t *testing.T) {
	const groups = 3
	const groupSize = 4
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = groups
	cfg.SimEndMs = 10000
	cfg.ServiceMs = 5
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.GroupSize = groupSize
	cfg.NumWorkers = 3
	cfg.Concurrency = 1
	cfg.PollMs = 10
	res := contractRun(t, cfg)
	if res.Stats.Completed != groups {
		t.Fatalf("Completed = %d, want %d (one join per all-success group)", res.Stats.Completed, groups)
	}
	if res.Stats.GroupJobsDone != groups*groupSize {
		t.Fatalf("GroupJobsDone = %d, want %d (member count)", res.Stats.GroupJobsDone, groups*groupSize)
	}
	if res.Stats.Failed != 0 || res.Stats.Retried != 0 {
		t.Fatalf("clean group run: Failed=%d Retried=%d, want 0", res.Stats.Failed, res.Stats.Retried)
	}
	if res.Stats.Started != groups {
		t.Fatalf("Started = %d, want %d (one per group burst item)", res.Stats.Started, groups)
	}
	if aps := res.Stats.AttemptsPerStart(); aps != float64(groupSize) {
		t.Fatalf("AttemptsPerStart = %.3f, want %.3f (GroupSize× per-member rate by convention)", aps, float64(groupSize))
	}
}

// TestGroupMemberFailureFailsGroup pins the getGroupStatus contract
// (Tasqueue/groups.go:125-137): a group is done only when EVERY member
// succeeded — the first terminal member failure fails the group, and the
// last member's terminal event suppresses the joiner's Completed even when
// the other members succeeded. Driving complete() directly controls which
// member fails (a timed-out attempt — no RNG).
func TestGroupMemberFailureFailsGroup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 100
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.GroupSize = 3
	rt, _ := contractRuntime(t, cfg)

	// Three members of group 0, all registered and in flight.
	const gid = uint64(0)
	for id := uint64(1); id <= 3; id++ {
		rt.jobs[id] = &jobState{id: id, chainIdx: -1, groupID: gid, attempt: 1, enqueuedMs: 0}
	}
	rt.groupRem[gid] = 3

	// Members 1 and 2 succeed; member 3 times out (terminal failure).
	rt.complete(model.Task{ID: 1}, rt.jobs[1], false)
	rt.complete(model.Task{ID: 2}, rt.jobs[2], false)
	rt.complete(model.Task{ID: 3}, rt.jobs[3], true)

	// The group never joins: one member failed → no Completed, and the
	// group bookkeeping is gone after the last terminal event (no leak).
	if rt.stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (group failed by member 3's timeout, groups.go:125-137)", rt.stats.Completed)
	}
	if rt.stats.Failed != 1 {
		t.Fatalf("Failed = %d, want 1", rt.stats.Failed)
	}
	if rt.stats.GroupJobsDone != 2 {
		t.Fatalf("GroupJobsDone = %d, want 2 (successful members)", rt.stats.GroupJobsDone)
	}
	if _, ok := rt.groupRem[gid]; ok {
		t.Fatalf("groupRem leak: group %d still tracked after last member terminated", gid)
	}
	if _, ok := rt.groupFail[gid]; ok {
		t.Fatalf("groupFail leak: group %d still tracked after last member terminated", gid)
	}
	if _, ok := rt.jobs[3]; ok {
		t.Fatalf("failed member still registered after terminal failure")
	}
}

// TestGroupExpiredMemberNeverJoins pins the expiry path of group
// accounting: a member whose queue residency exceeds the MaxWaitMs
// deadline is terminal (non-retryable), marks its group failed, and is
// removed from the registry — so a group with an expired member never
// joins and leaves no bookkeeping behind. The broker fully drains every
// pump tick (queue residency is ~0), so the expiry is driven on a manually
// advanced clock: the members sit in the queue past their deadline before
// the drain.
func TestGroupExpiredMemberNeverJoins(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 5
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.GroupSize = 3
	rt, sim := contractRuntime(t, cfg)
	rt.q.SetMaxWaitMs(40) // the deadline the runtime's broker queue carries

	// Three group members enqueued at t=0, left in the queue (no tick yet).
	const gid = uint64(0)
	for id := uint64(1); id <= 3; id++ {
		rt.jobs[id] = &jobState{id: id, chainIdx: -1, groupID: gid, attempt: 1, enqueuedMs: 0}
		rt.q.AddAt(model.Task{ID: id, Attempt: 1}, sim.Now())
	}
	rt.groupRem[gid] = 3

	// Advance past the deadline, then drain: all three expire.
	sim.Advance(50)
	rt.tick(sim.Now())

	if rt.stats.Expired != 3 {
		t.Fatalf("Expired = %d, want 3", rt.stats.Expired)
	}
	if rt.stats.Failed != 3 {
		t.Fatalf("Failed = %d, want 3 (expiry is terminal)", rt.stats.Failed)
	}
	if rt.stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (group never joins after expiry)", rt.stats.Completed)
	}
	if _, ok := rt.groupRem[gid]; ok {
		t.Fatalf("groupRem leak: group %d still tracked after all members expired", gid)
	}
	if _, ok := rt.groupFail[gid]; ok {
		t.Fatalf("groupFail leak: group %d still tracked after all members expired", gid)
	}
	if len(rt.jobs) != 0 {
		t.Fatalf("expired members still registered: jobs = %v", rt.jobs)
	}
}

// TestGroupRejectedMemberFailsGroup pins the capacity-reject × group
// intersection (the fourth member-destroying path, alongside
// drop-exhausted, expired and handler-exhausted): a capacity-full add of a
// group member is terminal for the member — it fails the group (a member
// that can never run is a failed member, getGroupStatus) and decrements
// the join counter, so the group bookkeeping never leaks and the group
// never joins.
func TestGroupRejectedMemberFailsGroup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 5
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.GroupSize = 3
	cfg.QueueCapacity = 2 // only two of three members fit
	rt, sim := contractRuntime(t, cfg)

	// Three members of group 0 registered; the third add is rejected.
	const gid = uint64(0)
	rt.groupRem[gid] = 3
	for id := uint64(1); id <= 3; id++ {
		js := &jobState{id: id, chainIdx: -1, groupID: gid, attempt: 1, enqueuedMs: 0}
		rt.jobs[id] = js
		rt.enqueueTask(model.Task{ID: id, Attempt: 1}, js, sim.Now())
	}
	if rt.stats.RejectedAdds != 1 {
		t.Fatalf("RejectedAdds = %d, want 1 (capacity 2 admits two of three members)", rt.stats.RejectedAdds)
	}
	if !rt.groupFail[gid] {
		t.Fatalf("rejected member did not fail its group (getGroupStatus)")
	}
	if rt.groupRem[gid] != 2 {
		t.Fatalf("groupRem = %d, want 2 (rejected member decremented the join counter)", rt.groupRem[gid])
	}
	if _, ok := rt.jobs[3]; ok {
		t.Fatal("rejected member still registered")
	}

	// The surviving members succeed → rem hits 0 → bookkeeping removed and
	// Completed suppressed by the failed member.
	rt.complete(model.Task{ID: 1}, rt.jobs[1], false)
	rt.complete(model.Task{ID: 2}, rt.jobs[2], false)
	if rt.stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (group contains a rejected member)", rt.stats.Completed)
	}
	if rt.stats.GroupJobsDone != 2 {
		t.Fatalf("GroupJobsDone = %d, want 2 (successful members)", rt.stats.GroupJobsDone)
	}
	if _, ok := rt.groupRem[gid]; ok {
		t.Fatalf("groupRem leak: group %d still tracked after the last member terminated", gid)
	}
	if _, ok := rt.groupFail[gid]; ok {
		t.Fatalf("groupFail leak: group %d still tracked after the last member terminated", gid)
	}
}

// TestDropConsumesRetryBudget pins the drop-path divergence documented on
// config.go DropRate: a drain-time delivery drop is modeled as an
// attempt-level failure consuming one retry. With DropRate=1.0 every queue
// entry is dropped on its drain: the first drop is retried (Retried++),
// the re-enqueued entry is dropped again and, with the budget exhausted,
// the job terminally fails (Failed++) — the handler never runs, so
// AttemptsTotal stays 0.
func TestDropConsumesRetryBudget(t *testing.T) {
	const burst = 20
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = burst
	cfg.SimEndMs = 2000
	cfg.ServiceMs = 5
	cfg.FailP = 0 // failures must come from the drop path, not RNG
	cfg.MaxRetries = 1
	cfg.DropRate = 1.0
	cfg.NumWorkers = 3
	cfg.Concurrency = 1
	cfg.PollMs = 10
	res := contractRun(t, cfg)
	if res.Stats.Retried != burst {
		t.Fatalf("Retried = %d, want %d (each job's first drop consumes its single retry)", res.Stats.Retried, burst)
	}
	if res.Stats.Failed != burst {
		t.Fatalf("Failed = %d, want %d (each job terminally fails on the second drop)", res.Stats.Failed, burst)
	}
	if res.Stats.AttemptsTotal != 0 {
		t.Fatalf("AttemptsTotal = %d, want 0 (a dropped delivery never reaches the handler)", res.Stats.AttemptsTotal)
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0", res.Stats.Completed)
	}
	if res.Stats.Dropped != 2*burst {
		t.Fatalf("Dropped = %d, want %d (two queue entries per job: first drop + retry drop)", res.Stats.Dropped, 2*burst)
	}
}

// TestCapacityRejectAccountingIdentity locks the reject boundary
// (documented on config.go QueueCapacity): a capacity-full add is counted
// in RejectedAdds and terminal for the job — it never completes and never
// fails, so the whole-run identity Started = Completed + Failed +
// RejectedAdds holds exactly (nothing else is outstanding at exhaustion in
// this no-retry, no-drop configuration).
func TestCapacityRejectAccountingIdentity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 5
	cfg.SimEndMs = 2000
	cfg.ServiceMs = 5
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.DropRate = 0
	cfg.QueueCapacity = 1 // first add fits; the rest are rejected
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 10
	res := contractRun(t, cfg)
	if res.Stats.RejectedAdds != 4 {
		t.Fatalf("RejectedAdds = %d, want 4 (capacity 1 admits one of a 5-job burst)", res.Stats.RejectedAdds)
	}
	if res.Stats.Completed != 1 {
		t.Fatalf("Completed = %d, want 1 (the admitted job is served)", res.Stats.Completed)
	}
	if res.Stats.Failed != 0 {
		t.Fatalf("Failed = %d, want 0 (rejects are terminal but not failures)", res.Stats.Failed)
	}
	closed := res.Stats.Started == res.Stats.Completed+res.Stats.Failed+res.Stats.RejectedAdds
	if !closed {
		t.Fatalf("accounting not closed: Started(%d) != Completed(%d)+Failed(%d)+RejectedAdds(%d)",
			res.Stats.Started, res.Stats.Completed, res.Stats.Failed, res.Stats.RejectedAdds)
	}
}

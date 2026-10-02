// Package tasqueue provides a faithful DES model of the Tasqueue job
// queue, service-ized on the GoDES model library (model.Service/System).
//
// Tasqueue (github.com/lawRathod/tasqueue) is a Redis/in-memory backed
// distributed job queue: producers Enqueue Jobs onto a Broker queue,
// consumers (workers) pull via Broker.Consume and process through
// Server.process with per-task concurrency, retry and timeout semantics.
// Jobs carry OnSuccess/OnError DAGs for chains (linked list via
// OnSuccess) and Groups (parallel fan-out) under Server.EnqueueChain /
// EnqueueGroup; scheduling via ETA / cron Schedule uses
// Broker.EnqueueScheduled.
//
// Architecture: two services — the queue service (the broker:
// "tasqueue:tasks" pending queue + internal ETA timer queue) and the worker
// service (the per-queue poller fleet, PollMs cadence, in-flight budget =
// Concurrency × NumWorkers). A shared mediator runtime drives both once per
// pump tick. The services register NO builder ports / Connect edges:
// dispatch (broker → fleet) and completion/retry/chain/onError (fleet →
// broker) flow through that mediator, so the generated DOT service graph
// (a structural-debug view of REGISTERED builder primitives only) renders
// empty clusters — "if the code doesn't traverse it, the diagram doesn't
// draw it" (honest schematics). The architecture figure is DERIVED FROM
// OBSERVED BEHAVIOR instead: set Config.Capture and the runtime records
// every mechanism transition at its real chokepoints into a model/capture
// Recorder (Topology.Capture); capture.Aggregate + Graph.WriteDOT render
// the mechanism map from what actually traversed the model. The recorded
// component names are this model's own (load/queue/dispatch/exec/terminal),
// keyed to the mapping below for the real-source pin.
//
// Mapping (component ↔ tasqueue source):
//
//	Broker queue (pending, Enqueue)     → model.TaskQueue (capacity,
//	  drain-time drop, MaxWaitMs)         — Tasqueue/brokers/in-memory/broker.go
//	EnqueueScheduled / ETA               → timer TaskQueue + tick drain
//	  (in-memory broker lacks it — brokers/in-memory EnqueueScheduled
//	  returns "does not support this method"; the model abstracts the
//	  Redis broker). ETA/cron are EXCLUDED from the bubble confirm matrix:
//	  confirm cells use ETAOffsetMs=0 and CronSchedule="" (doc-only — the
//	  bubble runs the real in-memory broker, which has nothing to hold
//	  scheduled jobs against).
//	Workers per queue (TaskOpts.Concurrency) → model.Poller (budget =
//	  Concurrency × NumWorkers, poll PollMs) — Tasqueue/server.go:299
//	Handler execution (ServiceMs)        → sim.ScheduleAt(ServiceMs)
//	  Timeout (JobOpts.Timeout, context.WithDeadline) → attempt fails iff
//	  ServiceMs > TimeoutMs (STRICT — equality does NOT time out;
//	  pinned by TestTimeoutBoundaryEquality) — Tasqueue/server.go:388
//	  execJob. The equality race is a bubble-envelope item: the real
//	  select may resolve either way when both arms are ready, so the
//	  bubble compares the boundary cell by envelope, not by pin.
//	Retry (MaxRetries, Retried, retryJob) → immediate re-enqueue (NO
//	  backoff), retry budget = MaxRetries  — Tasqueue/server.go:485
//	  The server's EFFECTIVE retry default is 0: the declared
//	  `defaultMaxRetry = 1` (Tasqueue/jobs.go:17) is dead code — NewJob
//	  (jobs.go:80-90) and DefaultMeta (jobs.go:64-76) never apply it, so a
//	  bare JobOpts{} carries MaxRetry 0 and the first handler error is
//	  TERMINAL (server.go:423 `MaxRetry != Retried` false at 0==0 →
//	  statusFailed :447). DefaultConfig().MaxRetries pins that effective
//	  default (0); retries require setting MaxRetries explicitly (the
//	  submodule's own examples pass bare JobOpts{} and run with zero
//	  retries; only its tests pass MaxRetries:1, jobs_test.go:153,201,280 —
//	  evidence: .agents/experiments/2026-09-06-tasqueue-effective-default-spike.md,
//	  WP-A1 real-server spike).
//	OnSuccess (chain)                    → success of job i enqueues job
//	  i+1                                 — Tasqueue/chains.go:48 NewChain
//	OnError                              → failure after retries exhausted
//	  enqueues OnError jobs               — Tasqueue/server.go:435
//	Group (parallel jobs)                → fan-out GroupSize jobs, join
//	  counter; completion mirrors getGroupStatus (groups.go:125-137): the
//	  group completes only when the LAST member succeeds AND no member
//	  failed (expiry is a member failure) —
//	  Tasqueue/groups.go:66 EnqueueGroup
//	Cron Schedule                        → fixed 200ms periodic burst
//
// Documented divergences: a drain-time delivery drop consumes one retry
// and tallies Retried/Failed per the budget (the real Retried increments
// only in retryJob after a handler error/timeout — server.go:492; the
// model keeps accounting closed — pinned by TestDropConsumesRetryBudget,
// which must keep passing unchanged); a capacity-full add is rejected and
// terminal for the job — failing its group if the job was a member (the
// real in-memory broker blocks producers —
// brokers/in-memory/broker.go:34,72); the Add-boundary guard lives in
// model/taskqueue.go AddAt (cap-full add returns false — every runtime
// enqueuer funnels through the same reject accounting, and
// TestCapacityRejectAccountingIdentity must keep passing). Confirm
// cells hold the burst under the 100-deep default; overflow is
// exercise-only in cell C4, whose verdict is fixed "divergence
// documented"; Started counts one per group burst
// item, so AttemptsPerStart for a GroupSize-member group run is
// GroupSize× the per-member rate by convention (Completed counts a group
// once, at the join).
//
// Usage:
//
//	cfg := tasqueue.DefaultConfig() // 20 jobs, 3 workers, MaxRetries 0 (effective server default)
//	cfg.MaxRetries = 1             // retries are opt-in: the declared defaultMaxRetry=1 is dead code
//	cfg.ChainLen = 3               // each burst job is a 3-step chain
//	res := tasqueue.Run(cfg)
//	fmt.Println(res.Stats.AttemptsPerStart(), res.Stats.ChainStepsDone)
//
// Determinism: seed names every substream (queue drop, fail draw). Same
// seed → bit-identical Result (Stats + monitor digest). Zero draws when a
// fault is off (DropRate 0, MaxWaitMs 0, FailP 0); a timed-out attempt
// skips the failP draw entirely. The substream keys are root-level
// ("tasqueue.fail", the queue's drop key), so compose at most one tasqueue
// System per core.Simulation.
package tasqueue

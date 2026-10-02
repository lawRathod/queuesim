package tasqueue

import (
	"fmt"
	"math"

	"github.com/lawRathod/queuesim/models/tasqueue/faults"
)

// Config configures the tasqueue model. Every mechanism knob has a zero
// default that disables it (neutrality contract); fields with non-zero
// defaults are applied in newRuntime. All latencies are sim ms.
//
// The knobs mirror the real Tasqueue server (github.com/lawRathod/tasqueue):
// JobOpts.MaxRetries, TaskOpts.Concurrency (per-queue, default GOMAXPROCS),
// JobOpts.Timeout (an attempt times out iff ServiceMs > TimeoutMs) and the
// OnSuccess/OnError DAGs. The old model's invented RetryInit/RetryCoeff
// backoff is dropped: retryJob re-enqueues IMMEDIATELY (server.go:485-510),
// so a failed attempt is re-queued at the same sim instant.
//
// MaxRetries semantics follow the server's EFFECTIVE default, not the dead
// declared constant: Tasqueue declares `defaultMaxRetry uint32 = 1`
// (jobs.go:17), but NewJob (jobs.go:80-90) and DefaultMeta (jobs.go:64-76)
// never read it — DefaultMeta copies JobOpts.MaxRetries (zero for a bare
// JobOpts{}) straight into Meta.MaxRetry (jobs.go:72). The first handler
// error is therefore TERMINAL (server.go:423 `msg.MaxRetry != msg.Retried`
// false at 0==0 → statusFailed :447). DefaultConfig() pins that effective
// default: MaxRetries 0. Callers wanting retries set MaxRetries explicitly
// (the submodule's own examples pass bare JobOpts{} and therefore run with
// zero retries; only its tests pass MaxRetries:1, jobs_test.go:153,201,280 —
// WP-A1 spike memo, .agents/experiments/2026-09-06-tasqueue-effective-default-spike.md).
type Config struct {
	// Seed names every substream (queue drop, fail draw). Same seed → same
	// bit-identical Result; 0 is a valid seed.
	Seed int64
	// BurstCount is the number of jobs (or groups) enqueued per burst;
	// 0 disables the burst inflow entirely.
	BurstCount int
	// SimEndMs is the pump horizon; the sim runs to queue exhaustion past
	// it (post-horizon tail events fire), so Result.SimEndMs is the true
	// ending clock.
	SimEndMs int64

	// Broker queue (tasqueue:tasks)
	// QueueCapacity bounds the pending queue; 0 = unbounded (default 100).
	// A capacity-full add is REJECTED: counted in RejectedAdds and terminal
	// for the job (never completed/failed; a rejected group member fails
	// its group — getGroupStatus). The real in-memory broker instead
	// BLOCKS producers when the channel is full
	// (brokers/in-memory/broker.go:34,72) — reject models an
	// admission-boundary loss, a deliberate divergence.
	QueueCapacity int
	// DropRate is the drain-time delivery drop probability [0,1]. A dropped
	// delivery is modeled as an attempt-level failure: it consumes one
	// retry and tallies Retried/Failed per the retry budget, but never
	// reaches the handler (AttemptsTotal is untouched). This is a
	// divergence — the real server's Retried increments only in retryJob
	// after a handler error/timeout (server.go:492) — chosen to keep the
	// accounting closed.
	DropRate  float64
	MaxWaitMs int64 // schedule-to-start deadline; 0 = off (expire → terminal)
	PollMs    int64 // worker poll cadence

	// Workers (one queue "tasqueue:tasks")
	NumWorkers int // fleet size (default 3)
	// Concurrency is the per-queue worker budget (TaskOpts.Concurrency).
	// The server's default is GOMAXPROCS (server.go:91-93); the model pins
	// 1 for machine-independent determinism. Confirm cells pin Concurrency
	// 1 explicitly (fail-fast: the bubble envelope asserts the same single
	// in-flight budget, so model and bubble compare the same closure).
	Concurrency int
	ServiceMs   int64 // per-attempt handler service time

	// Retry / timeout / failure
	FailP      float64 // per-attempt handler failure prob [0,1]
	MaxRetries uint32  // JobOpts.MaxRetries: retries AFTER the initial attempt. Default 0 = the server's EFFECTIVE default (a bare JobOpts{} has MaxRetry 0, so the first handler error is terminal — the declared defaultMaxRetry=1 at Tasqueue/jobs.go:17 is dead code; see DefaultConfig).
	TimeoutMs  int64   // JobOpts.Timeout; 0 = none (an attempt times out iff ServiceMs > TimeoutMs)

	// DAG
	ChainLen    int // per-burst job chain length (0 = no chain; >=2 links via OnSuccess)
	GroupSize   int // per-burst group fan-out (0 = no group; >0 spawns N jobs)
	OnErrorJobs int // jobs to enqueue on terminal failure (OnError list length)

	// Scheduling
	ScheduleEveryMs int64  // periodic enqueue period; 0 = burst only
	ETAOffsetMs     int64  // EnqueueScheduled offset for burst jobs
	CronSchedule    string // if set, burst chain head is cron-scheduled

	// DisableMonitor turns OFF the System's free monitor. Monitoring is
	// read-only and never perturbs the simulation, so an on/off system
	// yields a bit-identical simulation trace (determinism gate).
	DisableMonitor bool

	// LabeledMetrics turns ON the OPT-IN labeled/attributed metric surfaces
	// (per tasqueue queue). FALSE by default: registers NOTHING new, so the
	// determinism digest is unmoved; the labeled registration path is the one
	// to use for attribution figures. TRUE additionally registers the
	// load-vocabulary surfaces (model/monitor load vocabulary) — pure reads,
	// zero RNG draws, zero mutation, so the trace stays bit-identical. ADR-115.
	LabeledMetrics bool

	// Capture turns ON the runtime mechanism recorder (model/capture).
	// The model records transitions between its own component names at its
	// real chokepoints (load/queue/dispatch/exec); capture is observational
	// only — zero RNG draws, zero I/O inside Run, and never registered as a
	// topology or monitor node — so capture on/off leaves the simulation
	// trace and digest byte-identical. When enabled, flush the recorder
	// after Run (System().Recorder().FlushTo).
	Capture bool

	// Faults is the list of declarative, time-windowed fault injections.
	// Each fault overrides the corresponding static knob (DropRate /
	// FailP / TimeoutMs) while its [StartMs, EndMs) window is active and
	// restores the config baseline after. Faults may also be the ONLY
	// source of a knob (rate > 0 during the window, 0/off baseline) — the
	// inject → observe → recover story. An empty list (or a list whose
	// windows never activate) makes zero RNG draws and leaves the run
	// byte-identical to a config with no faults at all.
	Faults []faults.Fault
}

// DefaultConfig returns the configuration with server-default mechanisms
// where the real system defines them (queue "tasqueue:tasks", EFFECTIVE
// retry budget 0 — the declared defaultMaxRetry=1 at jobs.go:17 is dead
// code, see the Config.MaxRetries doc) and the historical calibration
// elsewhere.
func DefaultConfig() Config {
	return Config{
		Seed: 42, BurstCount: 20, SimEndMs: 2000,
		QueueCapacity: 100, PollMs: 10,
		NumWorkers: 3, Concurrency: 1, ServiceMs: 5,
		FailP: 0.30, MaxRetries: 0, // effective default: bare JobOpts{} → MaxRetry 0 (jobs.go:64-76,72; server.go:423,447)
		DropRate: 0, MaxWaitMs: 0,
		ChainLen: 0, GroupSize: 0, OnErrorJobs: 0,
	}
}

func (c Config) validate() error {
	if c.BurstCount < 0 {
		return fmt.Errorf("tasqueue: burst must be >=0")
	}
	if c.SimEndMs <= 0 {
		return fmt.Errorf("tasqueue: horizon must be >0")
	}
	if c.PollMs <= 0 {
		return fmt.Errorf("tasqueue: poll interval must be >0")
	}
	if c.FailP < 0 || c.FailP > 1 {
		return fmt.Errorf("tasqueue: failP %v out of [0,1]", c.FailP)
	}
	if c.DropRate < 0 || c.DropRate > 1 {
		return fmt.Errorf("tasqueue: drop rate %v out of [0,1]", c.DropRate)
	}
	if c.NumWorkers <= 0 {
		return fmt.Errorf("tasqueue: workers must be >0")
	}
	if c.Concurrency < 1 {
		return fmt.Errorf("tasqueue: concurrency must be >=1")
	}
	if c.ChainLen == 1 {
		return fmt.Errorf("tasqueue: chainLen 1 invalid (need >=2)")
	}
	if c.ServiceMs < 0 || c.TimeoutMs < 0 || c.MaxWaitMs < 0 || c.ETAOffsetMs < 0 || c.ScheduleEveryMs < 0 {
		return fmt.Errorf("tasqueue: latencies must be >=0")
	}
	for _, f := range c.Faults {
		if f.Kind != faults.KindDrop && f.Kind != faults.KindFailure && f.Kind != faults.KindTimeout {
			return fmt.Errorf("tasqueue: unknown fault kind %q", f.Kind)
		}
		if f.StartMs < 0 || f.EndMs < f.StartMs {
			return fmt.Errorf("tasqueue: fault [%d,%d) window invalid (0 <= Start <= End)", f.StartMs, f.EndMs)
		}
		if f.Rate < 0 {
			return fmt.Errorf("tasqueue: fault rate %v must be >=0", f.Rate)
		}
		if math.IsNaN(f.Rate) || math.IsInf(f.Rate, 0) {
			// NaN and ±Inf slip past the range checks (NaN < 0 / > 1 are
			// both false, and KindTimeout has no upper bound) but would
			// silently disable the fault — a NaN timeout rate flows into
			// int64(rate) with implementation-defined results.
			return fmt.Errorf("tasqueue: fault rate %v must be finite", f.Rate)
		}
		if (f.Kind == faults.KindDrop || f.Kind == faults.KindFailure) && f.Rate > 1 {
			return fmt.Errorf("tasqueue: %s rate %v out of [0,1]", f.Kind, f.Rate)
		}
	}
	return nil
}

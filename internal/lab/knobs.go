package lab

import (
	"fmt"
	"math"

	"github.com/lawRathod/queuesim/models/tasqueue"
	"github.com/lawRathod/queuesim/models/temporal"
)

// temporalKnobs is the curated knob surface for the Temporal model. Keys are
// stable wire names; the UI never sees the Go config.
var temporalKnobs = []Knob{
	{Key: "burst_count", Label: "Initial burst", Group: "Workload", Kind: "number",
		Help: "Workflows started at t=0.", Unit: "wf", Min: 1, Max: 500, Step: 1, Default: 50},
	{Key: "arrival_rate", Label: "Arrival rate", Group: "Workload", Kind: "slider",
		Help: "Sustained workflows per simulated second after the burst.", Unit: "/s", Min: 0, Max: 100, Step: 1, Default: 0},
	{Key: "arrival_rate2", Label: "Rate after switch", Group: "Workload", Kind: "slider",
		Help: "Second arrival rate applied after the switch time. 0 stops arrivals.", Unit: "/s", Min: 0, Max: 100, Step: 1, Default: 0},
	{Key: "arrival_switch_ms", Label: "Switch at", Group: "Workload", Kind: "number",
		Help: "When the second arrival rate takes over.", Unit: "ms", Min: 0, Max: 60000, Step: 500, Default: 0},
	{Key: "fanout_k", Label: "Fan-out width", Group: "Workload", Kind: "number",
		Help: "Child activities spawned per workflow on success (0 = single-activity lifecycle).", Unit: "tasks", Min: 0, Max: 10, Step: 1, Default: 0},
	{Key: "network_ms", Label: "Network latency", Group: "Workload", Kind: "number",
		Help: "Per-RPC propagation delay.", Unit: "ms", Min: 0, Max: 200, Step: 1, Default: 2},
	{Key: "activity_ms", Label: "Activity service time", Group: "Workload", Kind: "number",
		Help: "How long one activity execution takes.", Unit: "ms", Min: 0, Max: 1000, Step: 5, Default: 0},

	{Key: "pollers_wf", Label: "Workflow pollers", Group: "Matching", Kind: "slider",
		Help: "Long-polling workers parked on the workflow-task queue.", Min: 1, Max: 32, Step: 1, Default: 4},
	{Key: "pollers_act", Label: "Activity pollers", Group: "Matching", Kind: "slider",
		Help: "Long-polling workers parked on the activity-task queue.", Min: 1, Max: 32, Step: 1, Default: 4},
	{Key: "dispatch_rate", Label: "Dispatch throttle", Group: "Matching", Kind: "slider",
		Help: "Token-bucket rate on matching dispatch. 0 = unlimited.", Unit: "/s", Min: 0, Max: 100, Step: 1, Default: 0},
	{Key: "sync_match_window_ms", Label: "Sync-match window", Group: "Matching", Kind: "number",
		Help: "How long a poller must be parked before adds bypass sync-match and spool. Real server default: 200 ms.", Unit: "ms", Min: 0, Max: 500, Step: 10, Default: 200},
	{Key: "backlog_gate_ms", Label: "Backlog-age gate", Group: "Matching", Kind: "number",
		Help: "Disable the sync-match fast path once the head of the backlog is older than this. 0 = off.", Unit: "ms", Min: 0, Max: 10000, Step: 100, Default: 5000},
	{Key: "matching_capacity", Label: "Backlog capacity", Group: "Matching", Kind: "number",
		Help: "Spooled-backlog bound per class. 0 = unbounded.", Unit: "tasks", Min: 0, Max: 500, Step: 10, Default: 60},
	{Key: "child_drain_fraction", Label: "Child drain share", Group: "Matching", Kind: "slider",
		Help: "Local drain capacity of the activity write partition, as a fraction of the dispatch rate. 1 = no slowdown.", Min: 0, Max: 1, Step: 0.05, Default: 0.75},

	{Key: "cascade_bad_fraction", Label: "Bad-actor fraction", Group: "Failures", Kind: "slider",
		Help: "Fraction of workflows whose activities fail inside the fault window. -1 = off (no activity fails).", Min: -1, Max: 1, Step: 0.05, Default: -1},
	{Key: "fault_start_ms", Label: "Fault window start", Group: "Failures", Kind: "number",
		Help: "When the activity-failure window opens. 0 with end 0 means no failures.", Unit: "ms", Min: 0, Max: 60000, Step: 500, Default: 0},
	{Key: "fault_end_ms", Label: "Fault window end", Group: "Failures", Kind: "number",
		Help: "When the window closes. 0 = open-ended.", Unit: "ms", Min: 0, Max: 60000, Step: 500, Default: 0},
	{Key: "retry_max_attempts", Label: "Max attempts", Group: "Failures", Kind: "number",
		Help: "Activity attempt cap. 0 = unlimited (a failing workflow retries forever).", Min: 0, Max: 20, Step: 1, Default: 0},
	{Key: "retry_initial_ms", Label: "First retry backoff", Group: "Failures", Kind: "number",
		Help: "Delay before the first retry.", Unit: "ms", Min: 1, Max: 10000, Step: 50, Default: 100},
	{Key: "retry_coeff", Label: "Backoff multiplier", Group: "Failures", Kind: "slider",
		Help: "Each retry's backoff is multiplied by this.", Min: 1, Max: 3, Step: 0.1, Default: 1},

	{Key: "shards", Label: "History shards", Group: "History", Kind: "number",
		Help: "Range-owned persistence shards; each owns a slice of workflows.", Min: 1, Max: 16, Step: 1, Default: 1},
	{Key: "persist_ms", Label: "Persistence latency", Group: "History", Kind: "number",
		Help: "Write latency per history mutation.", Unit: "ms", Min: 0, Max: 100, Step: 1, Default: 0},

	{Key: "sim_end_ms", Label: "Horizon", Group: "Run", Kind: "number",
		Help: "How much simulated time to run.", Unit: "ms", Min: 1000, Max: 120000, Step: 1000, Default: 60000},
}

// tasqueueKnobs is the curated knob surface for the Tasqueue model.
var tasqueueKnobs = []Knob{
	{Key: "burst_count", Label: "Initial burst", Group: "Workload", Kind: "number",
		Help: "Jobs enqueued at t=0.", Unit: "jobs", Min: 1, Max: 500, Step: 1, Default: 20},
	{Key: "schedule_every_ms", Label: "Periodic enqueue", Group: "Workload", Kind: "number",
		Help: "Enqueue one more burst this often. 0 = burst only.", Unit: "ms", Min: 0, Max: 10000, Step: 100, Default: 0},
	{Key: "chain_len", Label: "Chain length", Group: "Workload", Kind: "number",
		Help: "Jobs linked by OnSuccess. 0 = plain jobs.", Min: 0, Max: 10, Step: 1, Default: 0},
	{Key: "group_size", Label: "Group size", Group: "Workload", Kind: "number",
		Help: "Jobs fanned out into each group; the group joins only if every member succeeds. 0 = none.", Min: 0, Max: 10, Step: 1, Default: 0},

	{Key: "num_workers", Label: "Workers", Group: "Workers", Kind: "slider",
		Help: "Consumer fleet size.", Min: 1, Max: 32, Step: 1, Default: 3},
	{Key: "concurrency", Label: "Concurrency", Group: "Workers", Kind: "number",
		Help: "Per-queue in-flight budget.", Min: 1, Max: 8, Step: 1, Default: 1},
	{Key: "service_ms", Label: "Handler time", Group: "Workers", Kind: "number",
		Help: "How long one handler attempt takes.", Unit: "ms", Min: 0, Max: 500, Step: 5, Default: 5},
	{Key: "poll_ms", Label: "Poll cadence", Group: "Workers", Kind: "number",
		Help: "How often a worker polls the broker.", Unit: "ms", Min: 1, Max: 1000, Step: 1, Default: 10},

	{Key: "fail_p", Label: "Failure probability", Group: "Reliability", Kind: "slider",
		Help: "Per-attempt handler failure probability.", Min: 0, Max: 1, Step: 0.05, Default: 0.3},
	{Key: "max_retries", Label: "Max retries", Group: "Reliability", Kind: "number",
		Help: "Retries after the first attempt. The real server's effective default is 0.", Min: 0, Max: 10, Step: 1, Default: 0},
	{Key: "drop_rate", Label: "Delivery drop rate", Group: "Reliability", Kind: "slider",
		Help: "Probability a delivery is dropped before reaching a handler (consumes a retry).", Min: 0, Max: 1, Step: 0.05, Default: 0},
	{Key: "max_wait_ms", Label: "Queue deadline", Group: "Reliability", Kind: "number",
		Help: "A job waiting longer than this expires. 0 = no deadline.", Unit: "ms", Min: 0, Max: 60000, Step: 500, Default: 0},
	{Key: "on_error_jobs", Label: "On-error jobs", Group: "Reliability", Kind: "number",
		Help: "Jobs enqueued when a job fails terminally (the OnError list).", Min: 0, Max: 10, Step: 1, Default: 0},

	{Key: "queue_capacity", Label: "Queue capacity", Group: "Broker", Kind: "number",
		Help: "Pending-queue bound. Adds at capacity are rejected.", Unit: "jobs", Min: 1, Max: 1000, Step: 10, Default: 100},

	{Key: "sim_end_ms", Label: "Horizon", Group: "Run", Kind: "number",
		Help: "How much simulated time to run.", Unit: "ms", Min: 1000, Max: 60000, Step: 1000, Default: 2000},
}

func temporalDefaults() map[string]float64 {
	c := temporal.DefaultConfig()
	return map[string]float64{
		"burst_count":          float64(c.BurstCount),
		"arrival_rate":         c.ArrivalRate,
		"arrival_rate2":        c.ArrivalRate2,
		"arrival_switch_ms":    float64(c.ArrivalSwitchMs),
		"fanout_k":             float64(c.FanoutK),
		"network_ms":           float64(c.NetworkMs),
		"activity_ms":          float64(c.ActivityMs),
		"pollers_wf":           float64(c.PollersWF),
		"pollers_act":          float64(c.PollersAct),
		"dispatch_rate":        c.DispatchRate,
		"sync_match_window_ms": float64(c.SyncMatchWindowMs),
		"backlog_gate_ms":      float64(c.BacklogGateMs),
		"matching_capacity":    float64(c.MatchingCapacity),
		"child_drain_fraction": c.ChildDrainFraction,
		"cascade_bad_fraction": c.CascadeBadFraction,
		"fault_start_ms":       float64(c.FaultStartMs),
		"fault_end_ms":         float64(c.FaultEndMs),
		"retry_max_attempts":   float64(c.RetryMaxAttempts),
		"retry_initial_ms":     float64(c.RetryInitialMs),
		"retry_coeff":          c.RetryCoeff,
		"shards":               float64(c.Shards),
		"persist_ms":           float64(c.PersistMs),
		"sim_end_ms":           float64(c.SimEndMs),
	}
}

func tasqueueDefaults() map[string]float64 {
	c := tasqueue.DefaultConfig()
	return map[string]float64{
		"burst_count":       float64(c.BurstCount),
		"schedule_every_ms": float64(c.ScheduleEveryMs),
		"chain_len":         float64(c.ChainLen),
		"group_size":        float64(c.GroupSize),
		"num_workers":       float64(c.NumWorkers),
		"concurrency":       float64(c.Concurrency),
		"service_ms":        float64(c.ServiceMs),
		"poll_ms":           float64(c.PollMs),
		"fail_p":            c.FailP,
		"max_retries":       float64(c.MaxRetries),
		"drop_rate":         c.DropRate,
		"max_wait_ms":       float64(c.MaxWaitMs),
		"on_error_jobs":     float64(c.OnErrorJobs),
		"queue_capacity":    float64(c.QueueCapacity),
		"sim_end_ms":        float64(c.SimEndMs),
	}
}

// applyTemporal turns a knob value set into a model config. Every knob in
// the curated surface is applied; anything not listed keeps the model's
// default (or the preset).
func applyTemporal(vals map[string]float64) temporal.Config {
	c := temporal.DefaultConfig()
	c.BurstCount = int(vals["burst_count"])
	c.ArrivalRate = vals["arrival_rate"]
	c.ArrivalRate2 = vals["arrival_rate2"]
	c.ArrivalSwitchMs = int64(vals["arrival_switch_ms"])
	c.FanoutK = int(vals["fanout_k"])
	c.NetworkMs = int64(vals["network_ms"])
	c.ActivityMs = int64(vals["activity_ms"])
	c.PollersWF = int(vals["pollers_wf"])
	c.PollersAct = int(vals["pollers_act"])
	c.DispatchRate = vals["dispatch_rate"]
	c.SyncMatchWindowMs = int64(vals["sync_match_window_ms"])
	c.BacklogGateMs = int64(vals["backlog_gate_ms"])
	c.MatchingCapacity = int(vals["matching_capacity"])
	c.ChildDrainFraction = vals["child_drain_fraction"]
	c.CascadeBadFraction = vals["cascade_bad_fraction"]
	c.FaultStartMs = int64(vals["fault_start_ms"])
	c.FaultEndMs = int64(vals["fault_end_ms"])
	c.RetryMaxAttempts = int32(vals["retry_max_attempts"])
	c.RetryInitialMs = int64(vals["retry_initial_ms"])
	c.RetryCoeff = vals["retry_coeff"]
	c.Shards = int(vals["shards"])
	c.PersistMs = int64(vals["persist_ms"])
	c.SimEndMs = int64(vals["sim_end_ms"])
	return c
}

func applyTasqueue(vals map[string]float64) tasqueue.Config {
	c := tasqueue.DefaultConfig()
	c.BurstCount = int(vals["burst_count"])
	c.ScheduleEveryMs = int64(vals["schedule_every_ms"])
	c.ChainLen = int(vals["chain_len"])
	c.GroupSize = int(vals["group_size"])
	c.NumWorkers = int(vals["num_workers"])
	c.Concurrency = int(vals["concurrency"])
	c.ServiceMs = int64(vals["service_ms"])
	c.PollMs = int64(vals["poll_ms"])
	c.FailP = vals["fail_p"]
	c.MaxRetries = uint32(vals["max_retries"])
	c.DropRate = vals["drop_rate"]
	c.MaxWaitMs = int64(vals["max_wait_ms"])
	c.OnErrorJobs = int(vals["on_error_jobs"])
	c.QueueCapacity = int(vals["queue_capacity"])
	c.SimEndMs = int64(vals["sim_end_ms"])
	return c
}

// validateKnobs rejects unknown keys, non-finite values and out-of-range
// values before a simulation is built.
func validateKnobs(defs []Knob, vals map[string]float64) error {
	known := make(map[string]Knob, len(defs))
	for _, k := range defs {
		known[k.Key] = k
	}
	for key, v := range vals {
		k, ok := known[key]
		if !ok {
			return fmt.Errorf("unknown knob %q", key)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("knob %q must be finite", key)
		}
		if v < k.Min || v > k.Max {
			return fmt.Errorf("knob %q = %g out of range [%g, %g]", key, v, k.Min, k.Max)
		}
	}
	return nil
}

// effectiveValues merges defaults ← preset ← user overrides.
func effectiveValues(defs []Knob, preset, user map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(defs))
	for _, k := range defs {
		out[k.Key] = k.Default
	}
	for k, v := range preset {
		out[k] = v
	}
	for k, v := range user {
		out[k] = v
	}
	return out
}

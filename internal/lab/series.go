package lab

// maxFrames bounds how many timeline points a run returns. The simulation
// still advances at its native cadence; sampling picks every stride-th tick
// so a 60 s run and a 5 s run both render smoothly.
const maxFrames = 1200

var temporalSeries = []SeriesMeta{
	{Key: "wf_depth", Label: "Workflow backlog", Unit: "tasks", Kind: "gauge"},
	{Key: "act_depth", Label: "Activity backlog (root)", Unit: "tasks", Kind: "gauge"},
	{Key: "child_depth", Label: "Activity backlog (child)", Unit: "tasks", Kind: "gauge"},
	{Key: "sticky_depth", Label: "Sticky cache", Unit: "tasks", Kind: "gauge"},
	{Key: "shard_transfer", Label: "Shard transfer queue", Unit: "tasks", Kind: "gauge"},
	{Key: "shard_timer", Label: "Shard timer queue", Unit: "tasks", Kind: "gauge"},
	{Key: "shard_resched", Label: "Shard reschedule queue", Unit: "tasks", Kind: "gauge"},
	{Key: "wf_pollers", Label: "Workflow pollers parked", Unit: "pollers", Kind: "gauge"},
	{Key: "act_pollers", Label: "Activity pollers parked", Unit: "pollers", Kind: "gauge"},
	{Key: "wf_inflight", Label: "Workflow tasks in flight", Unit: "tasks", Kind: "gauge"},
	{Key: "act_inflight", Label: "Activities in flight", Unit: "tasks", Kind: "gauge"},
	{Key: "wf_head_age_ms", Label: "Workflow backlog head age", Unit: "ms", Kind: "gauge"},
	{Key: "act_head_age_ms", Label: "Activity backlog head age", Unit: "ms", Kind: "gauge"},
	{Key: "child_head_age_ms", Label: "Child backlog head age", Unit: "ms", Kind: "gauge"},
	{Key: "started", Label: "Workflows started", Unit: "wf", Kind: "counter"},
	{Key: "activity_adds", Label: "Activity adds", Unit: "tasks", Kind: "counter"},
	{Key: "attempts_executed", Label: "Attempts executed", Unit: "tasks", Kind: "counter"},
	{Key: "completed", Label: "Workflows completed", Unit: "wf", Kind: "counter"},
	{Key: "retries", Label: "Retries", Unit: "tasks", Kind: "counter"},
	{Key: "failed", Label: "Workflows failed", Unit: "wf", Kind: "counter"},
	{Key: "sync_matched", Label: "Sync-matched", Unit: "tasks", Kind: "counter"},
	{Key: "spooled", Label: "Spooled to backlog", Unit: "tasks", Kind: "counter"},
	{Key: "dispatched", Label: "Dispatched", Unit: "tasks", Kind: "counter"},
	{Key: "rejected", Label: "Rejected adds", Unit: "tasks", Kind: "counter"},
	{Key: "dropped", Label: "Dropped", Unit: "tasks", Kind: "counter"},
	{Key: "expired", Label: "Expired", Unit: "tasks", Kind: "counter"},
	{Key: "started_rate", Label: "Started", Unit: "/s", Kind: "rate"},
	{Key: "completed_rate", Label: "Completed", Unit: "/s", Kind: "rate"},
	{Key: "activity_adds_rate", Label: "Activity adds", Unit: "/s", Kind: "rate"},
	{Key: "attempts_rate", Label: "Attempts executed", Unit: "/s", Kind: "rate"},
	{Key: "retries_rate", Label: "Retries", Unit: "/s", Kind: "rate"},
	{Key: "failed_rate", Label: "Failed", Unit: "/s", Kind: "rate"},
}

var temporalRateSources = map[string]string{
	"started_rate":       "started",
	"completed_rate":     "completed",
	"activity_adds_rate": "activity_adds",
	"attempts_rate":      "attempts_executed",
	"retries_rate":       "retries",
	"failed_rate":        "failed",
}

var temporalCharts = []Chart{
	{Title: "Matching backlog", Unit: "tasks", Series: []string{"wf_depth", "act_depth", "child_depth"},
		Note: "Tasks waiting when sync-match cannot serve them."},
	{Title: "Workers", Unit: "pollers", Series: []string{"wf_pollers", "act_pollers", "wf_inflight", "act_inflight"},
		Note: "Parked long-pollers versus executions in flight."},
	{Title: "Throughput", Unit: "/s", Series: []string{"started_rate", "completed_rate", "activity_adds_rate"}},
	{Title: "Failures and retries", Unit: "/s", Series: []string{"retries_rate", "failed_rate"}},
	{Title: "Backlog head age", Unit: "ms", Series: []string{"wf_head_age_ms", "act_head_age_ms", "child_head_age_ms"}},
	{Title: "History shard queues", Unit: "tasks", Series: []string{"shard_transfer", "shard_timer", "shard_resched"},
		Note: "Persistence-backed work: entering matching, scheduled timers, failed executions."},
}

var tasqueueSeries = []SeriesMeta{
	{Key: "queue_depth", Label: "Broker backlog", Unit: "jobs", Kind: "gauge"},
	{Key: "dispatch_depth", Label: "Dispatch buffer", Unit: "jobs", Kind: "gauge"},
	{Key: "timer_depth", Label: "Scheduled jobs", Unit: "jobs", Kind: "gauge"},
	{Key: "poller_inflight", Label: "Handlers in flight", Unit: "jobs", Kind: "gauge"},
	{Key: "started", Label: "Jobs started", Unit: "jobs", Kind: "counter"},
	{Key: "completed", Label: "Jobs completed", Unit: "jobs", Kind: "counter"},
	{Key: "retried", Label: "Attempts retried", Unit: "attempts", Kind: "counter"},
	{Key: "failed", Label: "Jobs failed", Unit: "jobs", Kind: "counter"},
	{Key: "dropped", Label: "Dropped", Unit: "jobs", Kind: "counter"},
	{Key: "expired", Label: "Expired", Unit: "jobs", Kind: "counter"},
	{Key: "rejected_adds", Label: "Rejected adds", Unit: "jobs", Kind: "counter"},
	{Key: "attempts_total", Label: "Handler attempts", Unit: "attempts", Kind: "counter"},
	{Key: "chain_steps_done", Label: "Chain steps done", Unit: "steps", Kind: "counter"},
	{Key: "group_jobs_done", Label: "Group members done", Unit: "jobs", Kind: "counter"},
	{Key: "started_rate", Label: "Started", Unit: "/s", Kind: "rate"},
	{Key: "completed_rate", Label: "Completed", Unit: "/s", Kind: "rate"},
	{Key: "attempts_rate", Label: "Handler attempts", Unit: "/s", Kind: "rate"},
	{Key: "retried_rate", Label: "Retried", Unit: "/s", Kind: "rate"},
	{Key: "failed_rate", Label: "Failed", Unit: "/s", Kind: "rate"},
}

var tasqueueRateSources = map[string]string{
	"started_rate":   "started",
	"completed_rate": "completed",
	"attempts_rate":  "attempts_total",
	"retried_rate":   "retried",
	"failed_rate":    "failed",
}

var tasqueueCharts = []Chart{
	{Title: "Broker backlog", Unit: "jobs", Series: []string{"queue_depth", "dispatch_depth", "timer_depth"},
		Note: "Pending work in the broker and the poll buffer."},
	{Title: "Cumulative work", Unit: "jobs", Series: []string{"started", "completed", "retried", "failed"},
		Note: "Totals since t=0. Retries count attempts; a chain job completes only at its tail."},
	{Title: "Throughput", Unit: "/s", Series: []string{"started_rate", "completed_rate", "attempts_rate"}},
	{Title: "Failures and retries", Unit: "/s", Series: []string{"failed_rate", "retried_rate"}},
}

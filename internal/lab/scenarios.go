package lab

import "sort"

// scenario is one curated preset. The Info fields are the public catalog
// entry; defs/preset/run are the executable half.
type scenario struct {
	id          string
	model       string
	group       string
	title       string
	subtitle    string
	blurb       string
	watch       []string
	defs        []Knob
	preset      map[string]float64
	defaultSeed int64
	run         func(vals map[string]float64, seed int64) (*Run, error)
}

func (s *scenario) info() Info {
	return Info{
		ID:          s.id,
		Model:       s.model,
		Group:       s.group,
		Title:       s.title,
		Subtitle:    s.subtitle,
		Blurb:       s.blurb,
		WhatToWatch: s.watch,
		Knobs:       s.defs,
		Preset:      effectiveValues(s.defs, s.preset, nil),
		DefaultSeed: s.defaultSeed,
		Charts:      chartsFor(s.model),
		Series:      seriesFor(s.model),
	}
}

func chartsFor(model string) []Chart {
	if model == "tasqueue" {
		return tasqueueCharts
	}
	return temporalCharts
}

func seriesFor(model string) []SeriesMeta {
	if model == "tasqueue" {
		return tasqueueSeries
	}
	return temporalSeries
}

// temporalScenarios are the curated Temporal experiments. Each one isolates
// a mechanism the model was built to expose.
var temporalScenarios = []*scenario{
	{
		id: "calm", model: "temporal", group: "Temporal", title: "Calm seas",
		subtitle: "A 50-workflow burst on a healthy cluster",
		blurb: "The baseline: every workflow start is sync-matched to a parked poller, the activity runs once, " +
			"and nothing queues. Watch how little the backlog lines move — this is what healthy looks like.",
		watch: []string{
			"Workflows complete in a single attempt (attempts per start ≈ 1.0).",
			"The matching backlog stays at zero — sync-match hands each task straight to a poller.",
			"Schedule-to-start latency is a few milliseconds.",
		},
		defs:        temporalKnobs,
		preset:      map[string]float64{"burst_count": 50, "sim_end_ms": 10000},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "steady-load", model: "temporal", group: "Temporal", title: "Steady load",
		subtitle: "20 workflows per second, no failures",
		blurb: "A sustained arrival stream. The system reaches a steady state where goodput equals the arrival rate. " +
			"Push the arrival rate or cut the poller count and watch the queues start to slope upward.",
		watch: []string{
			"Throughput lines flatten at the arrival rate — the system is keeping up.",
			"The poller lines saw-tooth as workers park, take, and park again.",
			"Raise arrival rate above what the pollers can serve and the backlog begins to grow.",
		},
		defs:        temporalKnobs,
		preset:      map[string]float64{"burst_count": 50, "arrival_rate": 20, "sim_end_ms": 20000},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "load-drop", model: "temporal", group: "Temporal", title: "Load drop, slow recovery",
		subtitle: "Arrivals fall from 40/s to 5/s while the backlog is still deep",
		blurb: "A 40/s spike fills the activity queue; at 10 s the arrival rate falls to 5/s. The backlog turns " +
			"down immediately, but the oldest task's wait keeps climbing until the queue shortens — the tail " +
			"outlives the load. This is why 'the load is gone' is not the same as 'the system has recovered'.",
		watch: []string{
			"The dashed marker at 10 s is the load step.",
			"Backlog depth falls from 10 s onward, but head age keeps rising for another ~10 s.",
			"Recovery is set by backlog depth ÷ spare drain rate, not by the arrival rate.",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 40, "arrival_rate2": 5, "arrival_switch_ms": 10000,
			"pollers_act": 1, "activity_ms": 50, "sim_end_ms": 30000,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "retry-storm", model: "temporal", group: "Temporal", title: "Retry storm",
		subtitle: "Half the workflows keep failing",
		blurb: "50% of workflows are bad actors: every activity attempt fails and is retried. Retries re-enter the same " +
			"matching queue, so a modest failure rate turns into several times the load. Attempts per start is the headline.",
		watch: []string{
			"Attempts per start climbs well above 1 — the retry amplification.",
			"The retries-per-second line roughly doubles the completed line.",
			"Failures only stop when the retry budget is exhausted (raise Max attempts).",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 10, "sim_end_ms": 30000,
			"cascade_bad_fraction": 0.5, "fault_start_ms": 1, "fault_end_ms": 0,
			"retry_max_attempts": 5, "retry_initial_ms": 100,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "outage", model: "temporal", group: "Temporal", title: "Outage and recovery",
		subtitle: "A 10-second failure window with exponential backoff",
		blurb: "Every activity fails between 2 s and 12 s. Retries back off exponentially, so the system damps its own " +
			"retry load instead of hammering the queue. When the window closes the remaining retries succeed and the system recovers.",
		watch: []string{
			"The shaded band is the fault window.",
			"During the band, retries dominate and completions stop.",
			"After the band, a burst of green completions drains the accumulated work.",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 20, "sim_end_ms": 30000,
			"cascade_bad_fraction": 1, "fault_start_ms": 2000, "fault_end_ms": 12000,
			"retry_max_attempts": 10, "retry_initial_ms": 200, "retry_coeff": 1.5,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "throttle", model: "temporal", group: "Temporal", title: "Dispatch throttle",
		subtitle: "Arrivals at 20/s, dispatch capped at 5/s",
		blurb: "A token bucket limits how fast matching dispatches tasks. Arrivals are four times the service rate, so the " +
			"backlog grows without bound. This is the clearest view of a saturated queue and its head-of-line age.",
		watch: []string{
			"Workflow backlog climbs in a straight line — arrivals minus the 5/s drain.",
			"Backlog head age grows with it: the oldest task waits longer and longer.",
			"Cut arrivals or raise the dispatch rate and the line bends back toward zero.",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 20, "dispatch_rate": 5, "sim_end_ms": 20000,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "fanout", model: "temporal", group: "Temporal", title: "Fan-out",
		subtitle: "Every workflow spawns four child activities",
		blurb: "A workflow that fans out to K children multiplies activity adds by K. With K=4 the activity load is five " +
			"times the workflow load, even though the arrival rate never changed.",
		watch: []string{
			"Activity adds per second settle at (K+1) × workflows started per second.",
			"The activity backlog is the first place congestion appears.",
			"Raise K and the multiplier follows.",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 10, "fanout_k": 4, "sim_end_ms": 20000,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
	{
		id: "bottleneck", model: "temporal", group: "Temporal", title: "Activity bottleneck",
		subtitle: "One activity poller against 40 workflows per second",
		blurb: "Each activity takes 50 ms and there is a single activity poller, so the activity queue can retire at most " +
			"20 tasks per second. Arrivals are double that: the activity backlog grows and schedule-to-start latency " +
			"climbs with it. The workflow queue stays empty because starts are cheap — the bottleneck is one layer down.",
		watch: []string{
			"The activity backlog climbs while the workflow backlog stays flat.",
			"Completed throughput pins at the poller's service rate (~20/s), not the arrival rate.",
			"Schedule-to-start p99 reaches seconds. Add pollers and watch both lines collapse.",
		},
		defs: temporalKnobs,
		preset: map[string]float64{
			"burst_count": 50, "arrival_rate": 40, "pollers_act": 1, "activity_ms": 50, "sim_end_ms": 20000,
		},
		defaultSeed: 42,
		run:         executeTemporal,
	},
}

// tasqueueScenarios exercise the simpler worker-pool model: a broker, a
// polling fleet, retries, and the DAG combinators.
var tasqueueScenarios = []*scenario{
	{
		id: "tasq-hello", model: "tasqueue", group: "Tasqueue", title: "Hello, job",
		subtitle: "20 jobs, three workers, no failures",
		blurb: "The smallest possible run: jobs land in the broker, three workers poll and execute them, everything " +
			"succeeds. This is the reference shape every other Tasqueue scenario deviates from.",
		watch: []string{
			"The broker backlog spikes at t=0 and drains at the worker service rate.",
			"Three handlers run concurrently — the in-flight line tops out at 3.",
			"Every job completes once, with no retries.",
		},
		defs:        tasqueueKnobs,
		preset:      map[string]float64{"burst_count": 20, "fail_p": 0, "service_ms": 5, "sim_end_ms": 3000},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-flaky", model: "tasqueue", group: "Tasqueue", title: "Flaky handler",
		subtitle: "40% of attempts fail, up to three retries",
		blurb: "Each attempt has a 40% chance of failing. With three retries most jobs eventually succeed, but the " +
			"attempt counter shows the hidden cost: attempts per start rises to about 1.6.",
		watch: []string{
			"Retried attempts appear as a second throughput line.",
			"Attempts per start is the retry amplification, not the failure rate.",
			"Set Max retries to 0 and watch the failure count jump: the first error becomes terminal.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 20, "fail_p": 0.4, "max_retries": 3, "service_ms": 5, "sim_end_ms": 5000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-overload", model: "tasqueue", group: "Tasqueue", title: "Worker overload",
		subtitle: "100 jobs, two workers, 50 ms each",
		blurb: "100 jobs arrive at once but two workers can only retire 40 jobs per second. The backlog is unbounded " +
			"unless a queue capacity or deadline is set — try lowering capacity and watch rejected adds appear.",
		watch: []string{
			"The broker backlog drains at exactly the worker service rate.",
			"Lower the queue capacity to see admission rejects.",
			"Give the queue a deadline to see waiting jobs expire instead of completing.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 100, "num_workers": 2, "service_ms": 50, "fail_p": 0, "sim_end_ms": 10000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-chain", model: "tasqueue", group: "Tasqueue", title: "Job chain",
		subtitle: "Four-step OnSuccess chains",
		blurb: "Each job enqueues its successor on success, so one logical chain is four sequential handler attempts. " +
			"Chain steps done counts the links; a failure anywhere aborts the rest of the chain.",
		watch: []string{
			"Chain steps accumulate in waves, one handler time apart.",
			"Set the chain length to 8 and the total run time doubles.",
			"Turn on failures: the first failed step ends its chain.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 10, "chain_len": 4, "fail_p": 0, "service_ms": 5, "sim_end_ms": 5000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-group", model: "tasqueue", group: "Tasqueue", title: "Job group",
		subtitle: "Five-member groups that join only if all succeed",
		blurb: "A group fans out into five jobs and joins when every member finishes. One failure fails the whole group, " +
			"so a 10% per-attempt failure rate is much worse for groups than for plain jobs.",
		watch: []string{
			"Five attempts run concurrently per group.",
			"Group members done rises in fives; completed groups rise more slowly.",
			"Raise the failure probability and watch groups fail as units.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 10, "group_size": 5, "fail_p": 0, "service_ms": 5, "sim_end_ms": 5000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-scheduled", model: "tasqueue", group: "Tasqueue", title: "Scheduled arrivals",
		subtitle: "A new burst every 500 ms",
		blurb: "Instead of one burst, a scheduler enqueues five jobs twice a second. This is the periodic-workload shape: " +
			"the backlog returns to zero between ticks if the workers keep up.",
		watch: []string{
			"The backlog saw-tooths instead of draining once.",
			"Throughput settles at the scheduler's average rate.",
			"Shorten the period below the service time and the saw-tooth turns into a growing backlog.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 5, "schedule_every_ms": 500, "fail_p": 0, "service_ms": 5, "sim_end_ms": 8000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
	{
		id: "tasq-poison", model: "tasqueue", group: "Tasqueue", title: "Poison pill",
		subtitle: "Every attempt fails; OnError spawns cleanup jobs",
		blurb: "A job that always fails exhausts its retry budget and goes terminal. Each terminal failure triggers the " +
			"OnError list, so a poison job still produces work — and still consumes attempts.",
		watch: []string{
			"All attempts fail; failed jobs accumulate.",
			"On-error jobs appear as extra started work after failures.",
			"Raise retries and the same jobs burn more attempts before giving up.",
		},
		defs: tasqueueKnobs,
		preset: map[string]float64{
			"burst_count": 10, "fail_p": 1, "max_retries": 2, "on_error_jobs": 2, "service_ms": 5, "sim_end_ms": 5000,
		},
		defaultSeed: 42,
		run:         executeTasqueue,
	},
}

var scenarios = func() map[string]*scenario {
	m := map[string]*scenario{}
	for _, list := range [][]*scenario{temporalScenarios, tasqueueScenarios} {
		for _, s := range list {
			m[s.id] = s
		}
	}
	return m
}()

// Catalog returns every scenario, grouped and sorted for the picker.
func Catalog() []Info {
	ids := make([]string, 0, len(scenarios))
	for id := range scenarios {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := scenarios[ids[i]], scenarios[ids[j]]
		if a.group != b.group {
			// Temporal first: it is the deeper model.
			if a.group == "Temporal" {
				return true
			}
			if b.group == "Temporal" {
				return false
			}
			return a.group < b.group
		}
		return a.id < b.id
	})
	out := make([]Info, 0, len(ids))
	for _, id := range ids {
		out = append(out, scenarios[id].info())
	}
	return out
}

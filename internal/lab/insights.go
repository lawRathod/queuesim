package lab

import "fmt"

// metricLookup reads a headline metric by key.
func metricLookup(metrics []Metric, key string) float64 {
	for _, m := range metrics {
		if m.Key == key {
			return m.Value
		}
	}
	return 0
}

func pct(v float64) string {
	if v >= 10 {
		return fmt.Sprintf("%.0f%%", v)
	}
	return fmt.Sprintf("%.1f%%", v)
}

// temporalInsights applies a small, deterministic rule set to the run's
// numbers. Each rule states a mechanism consequence, not a judgement: the
// dashboard should teach, not flatter.
func temporalInsights(vals map[string]float64, metrics []Metric, tb *traceBuilder, peakBacklog, peakHeadAge float64) []Insight {
	var out []Insight

	aps := metricLookup(metrics, "attempts_per_start")
	syncShare := metricLookup(metrics, "sync_share")
	arrival := vals["arrival_rate"]
	dispatch := vals["dispatch_rate"]
	gate := vals["backlog_gate_ms"]
	pollersAct := vals["pollers_act"]
	completed := metricLookup(metrics, "completed")
	started := metricLookup(metrics, "started")
	failed := metricLookup(metrics, "failed")
	parkedAct := tb.mean("act_pollers")
	inflightAct := tb.mean("act_inflight")

	if peakBacklog > 0 && dispatch > 0 && arrival > dispatch {
		out = append(out, Insight{
			Tone:  "warn",
			Title: "The dispatch throttle is the bottleneck",
			Detail: fmt.Sprintf("Arrivals are %.0f/s but matching only dispatches %.0f/s, so the backlog grows by about %.0f tasks every second. Raising poller counts will not help while the token bucket caps dispatch.",
				arrival, dispatch, arrival-dispatch),
		})
	}

	if peakBacklog > 0 && pollersAct > 0 && inflightAct >= 0.85*pollersAct && parkedAct < 0.5*pollersAct {
		out = append(out, Insight{
			Tone:   "warn",
			Title:  "The activity fleet is saturated",
			Detail: fmt.Sprintf("Nearly all %.0f activity pollers were busy (%.1f in flight on average) while the backlog was non-empty. A queue cannot drain faster than pollers × service time allow; add pollers or shorten the activity.", pollersAct, inflightAct),
		})
	}

	if aps > 1.05 {
		retries := metricLookup(metrics, "retries")
		fanout := vals["fanout_k"]
		switch {
		case retries > 0:
			out = append(out, Insight{
				Tone:   "warn",
				Title:  "Retry amplification",
				Detail: fmt.Sprintf("%.2f activity attempts per workflow started. A failure is re-added to the same matching queue, so retries compete with new work and consume the same poller capacity.", aps),
			})
		case fanout > 0:
			out = append(out, Insight{
				Tone:   "",
				Title:  "Fan-out multiplies activity work",
				Detail: fmt.Sprintf("%.2f activity adds per workflow: one initial activity plus %.0f children per success. Retry amplification is 1.0 here — the extra adds are structural, not retries.", aps, fanout),
			})
		}
	}

	if fs, fe := vals["fault_start_ms"], vals["fault_end_ms"]; (fs > 0 || fe > 0) && aps > 1.05 {
		who := "every workflow"
		if frac := vals["cascade_bad_fraction"]; frac >= 1 {
			who = "every workflow"
		} else if frac > 0 {
			who = fmt.Sprintf("%.0f%% of workflows", frac*100)
		}
		var detail string
		if fe > 0 {
			detail = fmt.Sprintf("Activities for %s fail from %.1f s until %.1f s. Retry traffic climbs inside the shaded band; when the band closes, the remaining retries succeed and throughput recovers.",
				who, float64(fs)/1000, float64(fe)/1000)
		} else {
			detail = fmt.Sprintf("Activities for %s fail from %.1f s for the rest of the run. Only the retry budget (or a shorter horizon) ends the failures.", who, float64(fs)/1000)
		}
		out = append(out, Insight{Tone: "warn", Title: "A fault window is driving the retries", Detail: detail})
	}

	if peakHeadAge > 0 && gate > 0 && peakHeadAge > gate {
		out = append(out, Insight{
			Tone:   "warn",
			Title:  "Head-of-line age crossed the backlog gate",
			Detail: fmt.Sprintf("The oldest backlogged task waited %.0f ms, past the %.0f ms gate. The sync-match fast path is disabled while the head is this old, so new adds are routed to the backlog.", peakHeadAge, gate),
		})
	}

	if syncShare >= 60 && peakBacklog == 0 && aps <= 1.05 {
		out = append(out, Insight{
			Tone:   "good",
			Title:  "Sync-match is doing its job",
			Detail: fmt.Sprintf("%s of tasks were handed directly to a parked poller and the backlog never formed. This is the healthy fast path.", pct(syncShare)),
		})
	} else if peakBacklog > 0 && syncShare < 25 {
		out = append(out, Insight{
			Tone:   "warn",
			Title:  "Most tasks are spooling",
			Detail: fmt.Sprintf("Only %s of tasks sync-matched; the rest queued. A long backlog or a short sync-match window pushes adds onto the slow path.", pct(syncShare)),
		})
	}

	if started > 0 && completed == started && failed == 0 {
		out = append(out, Insight{
			Tone:   "good",
			Title:  "No work was lost",
			Detail: "Every workflow that started completed. Nothing expired, exhausted its retries, or was dropped.",
		})
	}
	if failed > 0 {
		out = append(out, Insight{
			Tone:   "bad",
			Title:  "Some workflows never finished",
			Detail: fmt.Sprintf("%.0f of %.0f workflows failed (exhausted retries or expired). Check the failure window, retry budget, and schedule-to-start deadline.", failed, started),
		})
	}

	return trimInsights(out, 4)
}

// tasqueueInsights is the Tasqueue counterpart.
func tasqueueInsights(vals map[string]float64, metrics []Metric, tb *traceBuilder, peakBacklog float64) []Insight {
	var out []Insight
	started := metricLookup(metrics, "started")
	completed := metricLookup(metrics, "completed")
	failed := metricLookup(metrics, "failed")
	aps := metricLookup(metrics, "attempts_per_start")
	maxRetries := vals["max_retries"]
	workers := vals["num_workers"]
	concurrency := vals["concurrency"]
	service := vals["service_ms"]
	chain := vals["chain_len"]
	group := vals["group_size"]

	if failed > 0 {
		if maxRetries == 0 {
			out = append(out, Insight{
				Tone:   "bad",
				Title:  "The first error was terminal",
				Detail: "Max retries is 0 — the real Tasqueue server's effective default, because a bare JobOpts{} carries MaxRetry 0. A handler error goes straight to failed; raise Max retries to see retry behaviour.",
			})
		} else {
			out = append(out, Insight{
				Tone:  "bad",
				Title: "Jobs exhausted their retries",
				Detail: func() string {
					if failed > started {
						return fmt.Sprintf("%.0f jobs failed after exhausting their retry budget; on-error jobs spawned by those failures add more work to the same fleet.", failed)
					}
					return fmt.Sprintf("%.0f of %.0f jobs failed after exhausting their retry budget.", failed, started)
				}(),
			})
		}
	}

	if aps > 1.05 {
		detail := fmt.Sprintf("%.2f handler attempts per job started — the retry cost that a completion count hides.", aps)
		switch {
		case chain > 0:
			detail = fmt.Sprintf("A %d-step chain is %d sequential attempts, so attempts per start is inflated by design.", int(chain), int(chain))
		case group > 0:
			detail = fmt.Sprintf("A %d-member group is %d handler attempts per started group — fan-out, not retry amplification.", int(group), int(group))
		}
		out = append(out, Insight{Tone: "warn", Title: "Attempts exceed jobs", Detail: detail})
	}

	if peakBacklog > 0 && completed < started && failed == 0 {
		rate := 0.0
		if service > 0 {
			rate = workers * concurrency / (service / 1000)
		}
		detail := fmt.Sprintf("%.0f of %.0f jobs completed. The fleet drains at workers × concurrency ÷ handler time", completed, started)
		if rate > 0 {
			detail += fmt.Sprintf(" — about %.0f jobs/s here — so a backlog above that rate cannot drain before the horizon.", rate)
		}
		out = append(out, Insight{Tone: "warn", Title: "Workers could not keep up", Detail: detail})
	}

	if group > 0 {
		out = append(out, Insight{
			Tone:   "",
			Title:  "Groups fail as units",
			Detail: fmt.Sprintf("A group joins only when all %d members succeed; one failed member fails the whole group.", int(group)),
		})
	}

	if vals["drop_rate"] > 0 {
		out = append(out, Insight{
			Tone:   "warn",
			Title:  "Drops consume retries silently",
			Detail: "A dropped delivery consumes a retry budget but never reaches a handler, so the attempt count and the failure count diverge.",
		})
	}

	if started > 0 && completed == started && failed == 0 {
		out = append(out, Insight{Tone: "good", Title: "Every job completed", Detail: "No job failed, expired, or was rejected."})
	}

	if peakBacklog == 0 && started > 0 && vals["schedule_every_ms"] > 0 {
		out = append(out, Insight{Tone: "good", Title: "The fleet keeps up between bursts", Detail: "The backlog returns to zero between scheduled bursts — the fleet's capacity exceeds the periodic load."})
	}

	return trimInsights(out, 4)
}

func trimInsights(in []Insight, max int) []Insight {
	if len(in) > max {
		return in[:max]
	}
	return in
}

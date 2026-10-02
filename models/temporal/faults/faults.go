// Package faults implements the temporal model's decision-vector fault
// application (ADR-078) as a self-contained subpackage. Each fault kind
// maps to a real chokepoint on the model's runtime, and the application
// logic for each kind lives in its own file here, operating ONLY through
// the Target interface — this package never touches the model's unexported
// internals.
//
// Traceability label: every kind here is Fault-control (taxonomy in
// models/temporal/doc.go) — a decision-vector test knob applied at a real
// chokepoint, not a server pin. No constant in this package pins a
// `temporal/` submodule source.
//
// File map:
//
//	add_drop.go          add_drop          spool-time drop on both matching queues (transport loss)
//	wf_drop.go           wf_drop           spool-time drop on matching-wf only (class starvation)
//	poll_drop.go         poll_drop         dispatch-rate collapse (token bucket → reduced rate)
//	s2s_timeout.go       s2s_timeout       MaxWaitMs deadline on matching-act (S2S)
//	transfer_delay.go    transfer_delay    dispatch delay on both matching queues (latency)
//	poller_min.go        poller_min        forced low poller count (worker starvation)
//	timer_load.go        timer_load        per-workflow timer before the activity
//	activity_failure.go  activity_failure  activity failure (ActivityFailsAt/ActivityFailsFor)
//	persistence_stall.go persistence_stall persistence latency multiplier
//	poll_jitter.go       poll_jitter       poller long-poll early-expiry probability
//	s2c_shorten.go       s2c_shorten       ScheduleToClose override
//	backlog_surge.go     backlog_surge     surge scheduled at Run time by the model (no pump-time op)
//
// Apply is the pump-time dispatcher: it accumulates each active member's
// knob — max-rate across active members for the drop kinds (add_drop,
// wf_drop, poll_drop), last-wins for the scalar kinds (s2s_timeout,
// transfer_delay, poller_min, timer_load, persistence_stall, poll_jitter,
// s2c_shorten; activity_failure is any-active at query time and
// backlog_surge has no pump-time op) — and hands off to the per-kind
// application functions in the sibling files (file map above).
package faults

import "github.com/lawRathod/queuesim/model"

// Apply applies decision-vector knobs at the pump (ADR-078). This function
// is the switch + per-kind accumulation only — each kind's application
// lives in its own file (file map above). The temporal model's runtime
// calls this once per pump tick with itself as the Target and a Spec
// projected from its Config (fault_target.go); the duplicate call at start
// (construction-time knob application) is intentional and idempotent.
func Apply(t Target, spec Spec, nowMs int64) {
	v := spec.Vector
	if v == nil {
		return
	}
	pollerForced := -1
	timerForced := int64(-1)
	transferDelay := int64(-1)
	s2sTimeout := int64(-1)
	addDrop := -1.0
	wfDrop := -1.0
	pollDrop := -1.0
	for _, m := range v.Members() {
		if !m.ActiveAt(nowMs) {
			continue
		}
		switch m.Kind {
		case model.KindAddDrop:
			if m.Rate > addDrop {
				addDrop = m.Rate
			}
		case model.KindWfDrop:
			if m.Rate > wfDrop {
				wfDrop = m.Rate
			}
		case model.KindPollDrop:
			if m.Rate > pollDrop {
				pollDrop = m.Rate
			}
		case model.KindS2STimeout:
			s2sTimeout = int64(m.Rate)
		case model.KindTransferDelay:
			transferDelay = int64(m.Rate)
		case model.KindPollerMin:
			pollerForced = int(m.Rate)
		case model.KindTimerLoad:
			timerForced = int64(m.Rate)
		case model.KindActivityFailure:
			// Handled in ActivityFailsAt/ActivityFailsFor
			// (activity_failure.go).
		case model.KindPersistenceStall:
			applyPersistenceStall(t, m.Rate)
		case model.KindPollJitter:
			applyPollJitter(t, m.Rate)
		case model.KindS2CShorten:
			applyS2CShorten(t, m.Rate)
		case model.KindBacklogSurge:
			// No pump-time op: the surge is scheduled at Run time by
			// scheduleBacklogSurge (models/temporal/load.go).
		}
	}
	applySpoolDrop(t, addDrop, wfDrop)
	applyDispatchRate(t, spec, pollDrop)
	applyS2STimeout(t, spec, s2sTimeout)
	applyTransferDelay(t, transferDelay)
	applyPollerMin(t, pollerForced)
	applyTimerLoad(t, timerForced)
}

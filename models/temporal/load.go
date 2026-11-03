package temporal

import (
	"math"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/model"
)

// This file owns the WORKLOAD SOURCE: the initial burst, any sustained
// arrivals, and the backlog-surge vector. These are runtime scheduling
// behaviors — they fire workflow starts at the right sim times — not a
// Service. There is deliberately NO frontend service: in the mechanical
// model the StartWorkflow RPC adds no chip (its latency is folded into the
// startWorkflow atMs as NetworkMs), so a frontend service would be a pure
// schematic decoration. If a future model needs the frontend's RPC hop or
// API surface (signals, visibility), re-introduce it as a real service with
// an in/out port pair and an actual network edge; until then history.start
// is the drawn entry point.
func (rt *runtime) scheduleBurst(now core.SimTime) {
	rt.sim.ScheduleAt(core.SimTime(rt.cfg.BurstAtMs), func(s *core.Simulation) {
		now := s.Now()
		for i := 0; i < rt.cfg.BurstCount; i++ {
			rt.stats.Started++
			rt.startWorkflow(Task{ID: rt.nextID, Attempt: 1, Class: TaskWfStart, BornMs: now.Int64()}, now.Add(rt.cfg.NetworkMs))
			rt.nextID++
		}
	})
}

// scheduleArrival self-reschedules one workflow arrival per exponential
// draw (ArrivalRate workflows/s). Zero draws when off (arrivalRNG nil).
func (rt *runtime) scheduleArrival() {
	rt.sim.ScheduleIn(0, func(s *core.Simulation) {
		now := s.Now()
		if now.Int64() >= rt.endMs {
			return
		}
		rt.stats.Started++
		rt.startWorkflow(Task{ID: rt.nextID, Attempt: 1, Class: TaskWfStart, BornMs: now.Int64()}, now.Add(rt.cfg.NetworkMs))
		rt.nextID++
		interval := -math.Log(1-rt.arrivalRNG.Float64()) / rt.cfg.ArrivalRate * 1000
		delay := int64(math.Round(interval))
		if delay < 1 {
			delay = 1
		}
		rt.sim.ScheduleIn(delay, func(*core.Simulation) { rt.scheduleArrival() })
	})
}

// scheduleBacklogSurge schedules a secondary burst of Rate workflows at
// each BacklogSurge vector member's StartMs (a load spike on top of the
// primary burst). KindBacklogSurge has NO pump-time operation — the faults
// subpackage's Apply deliberately no-ops it (faults/backlog_surge.go); the
// surge is a Run-time scheduling concern, handled here once from
// runtime.start.
//
// NOTE (schedule path): schedule-driven backlog_surge is a documented
// follow-on gap, NOT this WP — this function keeps reading cfg.Vector
// only. A shared fault schedule (Config.FaultSchedule) carrying a
// backlog_surge window does NOT drive a surge on the model leg yet
// (fault_target.go applySchedule no-ops KindBacklogSurge); wiring the
// surge to the schedule is a later WP.
func (rt *runtime) scheduleBacklogSurge(now core.SimTime) {
	if rt.cfg.Vector == nil {
		return
	}
	for _, m := range rt.cfg.Vector.Members() {
		if m.Kind == model.KindBacklogSurge && m.Rate > 0 {
			surge := int(m.Rate)
			atMs := m.StartMs
			rt.sim.ScheduleAt(core.SimTime(atMs), func(s *core.Simulation) {
				now := s.Now()
				for i := 0; i < surge; i++ {
					rt.stats.Started++
					rt.startWorkflow(Task{ID: rt.nextID, Attempt: 1, Class: TaskWfStart, BornMs: now.Int64()}, now.Add(rt.cfg.NetworkMs))
					rt.nextID++
				}
			})
		}
	}
}

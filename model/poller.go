package model

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// PollerConfig configures a Poller.
type PollerConfig struct {
	// Name is the poller identity (used in substream keys if it draws).
	Name string
	// PollIntervalMs is the self-rescheduling poll period.
	PollIntervalMs int64
	// InFlightBudget caps concurrent in-flight tasks per poller; 0 =
	// unlimited.
	InFlightBudget int
	// EndMs stops the poller's self-rescheduling at or after this sim time.
	// 0 = never stop (callers that stop elsewhere must still bound it).
	EndMs int64
}

// Poller is an automaton consumer (ADR-068): it self-reschedules poll
// events every PollIntervalMs and, while under its in-flight budget, calls
// the take hook — the topology's dispatch boundary (matching → execution).
// Service completions are the topology's job; the poller only tracks
// in-flight accounting via Release.
type Poller struct {
	sim      *core.Simulation
	cfg      PollerConfig
	inFlight int
	polls    uint64
	stopped  bool // set when a poll skipped at EndMs; cleared by Revive
	take     func(now core.SimTime) bool
}

// NewPoller creates a Poller. take is invoked on each eligible poll event;
// returning true means a task was taken (the poller increments in-flight).
// take may be nil for cadence-only pollers.
func NewPoller(sim *core.Simulation, cfg PollerConfig, take func(now core.SimTime) bool) *Poller {
	if sim == nil {
		panic("model: nil simulation in NewPoller")
	}
	return &Poller{sim: sim, cfg: cfg, take: take}
}

// Start schedules the first poll at the current sim time. Idempotent.
func (p *Poller) Start() { p.schedulePoll(0) }

// SetTake installs or replaces the take hook. Must be called before Start.
func (p *Poller) SetTake(take func(now core.SimTime) bool) { p.take = take }

// SetEndMs sets the stop horizon (plan 2026-08-12 D4): the next poll event
// at or after endMs skips its take and stops self-rescheduling (at-pop-skip
// semantics). 0 = never stop. Idempotent and deterministic — the stop
// decision is a pure comparison at the poll event.
func (p *Poller) SetEndMs(endMs int64) { p.cfg.EndMs = endMs }

// EndMs returns the current stop horizon (0 = never stop) — the D4
// controller's observability seam.
func (p *Poller) EndMs() int64 { return p.cfg.EndMs }

// Stopped reports whether the poller has hit its EndMs and stopped
// self-rescheduling (the D4 controller's ActivePollers seam).
func (p *Poller) Stopped() bool { return p.stopped }

// Revive re-arms a poller stopped by SetEndMs (plan 2026-08-12 D4): clears
// the stopped flag and schedules the next poll. No-op while the poller is
// still running — event chains are never duplicated.
func (p *Poller) Revive() {
	if !p.stopped {
		return
	}
	p.stopped = false
	p.schedulePoll(p.cfg.PollIntervalMs)
}

// Release decrements in-flight (called by the topology when an attempt
// completes). Over-release is a no-op.
func (p *Poller) Release() {
	if p.inFlight > 0 {
		p.inFlight--
	}
}

// InFlight returns the current in-flight task count.
func (p *Poller) InFlight() int { return p.inFlight }

// PollCount returns the number of poll events fired.
func (p *Poller) PollCount() uint64 { return p.polls }

// schedulePoll fires one poll event after delayMs, then chains the next.
func (p *Poller) schedulePoll(delayMs int64) {
	p.sim.ScheduleIn(delayMs, func(s *core.Simulation) {
		now := s.Now()
		if p.cfg.EndMs > 0 && now.Int64() >= p.cfg.EndMs {
			p.stopped = true
			return // stop self-rescheduling at the configured horizon
		}
		p.stopped = false
		p.polls++
		if p.cfg.InFlightBudget == 0 || p.inFlight < p.cfg.InFlightBudget {
			if p.take != nil && p.take(now) {
				// In-flight accounting applies only under a budget: a
				// budget-0 poller's take is synchronous by contract (the
				// wf-class pollers schedule their next step inside the
				// take), so there is no in-flight period to track.
				if p.cfg.InFlightBudget != 0 {
					p.inFlight++
				}
			}
		}
		p.schedulePoll(p.cfg.PollIntervalMs)
	})
}

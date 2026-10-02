package worker

import (
	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
)

// Task alias — canonical is matcher.Task.
type Task = matcher.Task

// Pool models a fleet of workers that long-poll a MatchQueue.
type Pool struct {
	sim       *core.Simulation
	queue     *matcher.MatchQueue
	serviceMs int64
	budget    int
	inFlight  int
	onDone    func()
}

// NewPool builds a pool servicing queue q.
func NewPool(sim *core.Simulation, q *matcher.MatchQueue, serviceMs int64, budget int) *Pool {
	if serviceMs < 0 {
		serviceMs = 0
	}
	if budget < 0 {
		budget = 0
	}
	return &Pool{sim: sim, queue: q, serviceMs: serviceMs, budget: budget}
}

// InFlight returns the current execution count.
func (p *Pool) InFlight() int { return p.inFlight }

// Budget returns the per-worker cap.
func (p *Pool) Budget() int { return p.budget }

// SetOnDone installs the completion hook.
func (p *Pool) SetOnDone(fn func()) { p.onDone = fn }

// Start begins an execution.
func (p *Pool) Start(t Task, now core.SimTime) core.SimTime {
	p.inFlight++
	at := now.Add(p.serviceMs)
	p.sim.ScheduleAt(at, func(*core.Simulation) {
		p.inFlight--
		if p.onDone != nil {
			p.onDone()
		}
	})
	return at
}

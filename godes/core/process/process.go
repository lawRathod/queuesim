// Package process provides process-based (coroutine/`yield`) modeling on
// top of the GoDES core scheduler. A process is a straight-line model
// body that suspends at calls to Wait / WaitUntil; the engine advances it
// when the sim clock reaches the requested time.
//
// Processes are implemented with runtime goroutine yields via the
// standard library's iter.Pull (Go 1.23+), so the body runs inline on the
// simulation goroutine and suspends exactly at a yield. There are no
// user-visible goroutines, no shared state, and no races — the simulation
// remains deterministic under a fixed seed.
//
// Contract for body authors:
//   - Call Wait(delay) / WaitUntil(t) / Yield() to suspend; every path
//     through the body must eventually wait or return. A body that loops
//     forever without waiting never returns from the first Wait and will
//     hang the simulation (the engine cannot preempt it).
//   - Do NOT call sim.Run() / sim.Step() / sim.Advance() from inside a
//     body — that would drive the scheduler from within a scheduled
//     handler.
package process

import (
	"iter"

	"github.com/lawRathod/queuesim/godes/core"
)

// Op is the internal communication unit between a suspended process body
// and the engine: what the body is waiting for before it resumes.
type Op interface {
	isOp()
}

// OpWaitDelay requests suspension for a number of sim ticks.
type OpWaitDelay struct {
	Duration int64
}

func (OpWaitDelay) isOp() {}

// OpWaitUntil requests suspension until an absolute sim time.
type OpWaitUntil struct {
	At core.SimTime
}

func (OpWaitUntil) isOp() {}

// OpEnd signals that the body has completed (returned).
type OpEnd struct{}

func (OpEnd) isOp() {}

// Env is passed to a process body. Its Wait / WaitUntil / Yield methods
// suspend the body until the engine resumes it.
type Env struct {
	sim      *core.Simulation
	yield    func(Op) bool
	finished bool
}

// Wait suspends the process for delay sim ticks (negative delays are
// clamped to 0). Returns when the sim clock reaches now+delay.
func (e *Env) Wait(delay int64) {
	if delay < 0 {
		delay = 0
	}
	e.emit(OpWaitDelay{Duration: delay})
}

// WaitUntil suspends the process until the absolute sim time at. If at is
// in the past relative to now, the process resumes immediately (at the
// current time).
func (e *Env) WaitUntil(at core.SimTime) {
	e.emit(OpWaitUntil{At: at})
}

// Yield suspends the process for zero sim time — a cooperative yield that
// lets other ready processes / events run before this one resumes. Useful
// for interleaving processes at the same sim time.
func (e *Env) Yield() {
	e.emit(OpWaitDelay{Duration: 0})
}

// Now returns the current simulation time.
func (e *Env) Now() core.SimTime { return e.sim.Now() }

// Finished reports whether the body has completed (returned).
func (e *Env) Finished() bool { return e.finished }

// emit triggers the pending op, suspending the body until the engine
// resumes it. It is only valid while the body is running (inside a next()
// call), and it returns after the next resume.
func (e *Env) emit(op Op) {
	if e.finished {
		return
	}
	if !e.yield(op) {
		// yield returned false => the iterator was stopped (teardown);
		// treat as end-of-process.
		e.finished = true
	}
}

// Process drives one process body to completion through the core
// scheduler. Create with New, then call Start to begin it.
type Process struct {
	env  *Env
	next func() (Op, bool)
	stop func()
}

// New creates a Process from a straight-line body. Construction does not
// begin execution; call Start (or StartAt) to schedule the first run.
func New(sim *core.Simulation, body func(*Env)) *Process {
	if sim == nil {
		panic("process: New requires a non-nil simulation")
	}
	if body == nil {
		panic("process: New requires a non-nil body")
	}
	e := &Env{sim: sim}
	nxt, stp := newSeq(body, e)
	e.yield = nil // assigned inside the coroutine on first run
	return &Process{env: e, next: nxt, stop: stp}
}

// newSeq wires the body into a pull-style iterator over Ops.
func newSeq(body func(*Env), e *Env) (func() (Op, bool), func()) {
	return iter.Pull(func(yield func(Op) bool) {
		e.yield = yield
		defer func() {
			e.finished = true
		}()
		body(e)
		yield(OpEnd{})
	})
}

// Start begins the process at the current simulation time. Idempotent
// after the process has completed (a finished process ignores Start).
func (p *Process) Start() {
	p.resume()
}

// resume advances the process body until it produces its next op, then
// schedules the continuation on the scheduler.
func (p *Process) resume() {
	op, ok := p.next()
	if !ok {
		p.env.finished = true
		return
	}
	switch o := op.(type) {
	case OpWaitDelay:
		p.scheduleIn(o.Duration)
	case OpWaitUntil:
		p.scheduleTo(o.At)
	case OpEnd:
		p.env.finished = true
		// body returned and yielded OpEnd; nothing further to schedule.
	default:
		panic("process: unknown op")
	}
}

// scheduleIn schedules the next resume delta ticks from now.
func (p *Process) scheduleIn(delta int64) {
	at := p.env.sim.Now().Add(delta)
	p.scheduleAt(at)
}

// scheduleTo schedules the next resume at or after the absolute time at,
// clamping to the current time if at is in the past.
func (p *Process) scheduleTo(at core.SimTime) {
	now := p.env.sim.Now()
	if at < now {
		at = now
	}
	p.scheduleAt(at)
}

// scheduleAt registers a resumption of the body at sim time at.
func (p *Process) scheduleAt(at core.SimTime) {
	p.env.sim.ScheduleAt(at, func(s *core.Simulation) {
		if p.env.finished {
			return
		}
		p.resume()
	})
}

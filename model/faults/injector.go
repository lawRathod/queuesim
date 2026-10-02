// Package faults provides a first-class, graph-agnostic fault-injection
// DSL for GoDES models. An Injector attaches to a model component (via a
// Target adapter — see QueueTarget history in model.TaskQueue knobs) and
// applies declarative, time-windowed faults: drops, delays, and residency
// timeouts. Faults are composable (multiple injections can target the same
// component) and deterministic (the underlying component draws from its
// own per-name substream).
//
// The fault model is window-based: each Injection declares [Start, Start+
// Duration) and a rate/amount. When the window is inactive, the component
// behaves identically to having no fault — zero RNG draws, zero behavior
// change (the neutrality contract). A knob with no active window is reset
// to off, so an Injector never leaves stale fault state behind.
package faults

import "github.com/law/GoDES/core"

// Kind identifies a fault type.
type Kind string

const (
	// KindDrop drops a fraction of tasks passing through the target
	// (probability rate). Supported by TaskQueue (drain-time drop via
	// SetDropRate).
	KindDrop Kind = "drop"
	// KindDelay delays each task by a fixed amount (rate = delay in sim
	// ticks). Supported by TaskQueue (SetDelay + SetDelayActive).
	KindDelay Kind = "delay"
	// KindTimeout applies a maximum-residency deadline (rate = deadline in
	// sim ticks): tasks whose queue residency exceeds it expire.
	// Supported by TaskQueue (SetMaxWaitMs).
	KindTimeout Kind = "timeout"
)

// Injection is one declarative, time-windowed fault.
type Injection struct {
	// Kind is the fault kind.
	Kind Kind
	// Start is the window start in sim ticks.
	Start int64
	// Duration is the window length in sim ticks; 0 = open-ended from
	// Start.
	Duration int64
	// Rate is kind-specific: drop probability [0,1], delay ticks, or
	// timeout deadline ticks.
	Rate float64
	// Name identifies the injection (diagnostics).
	Name string
}

// ActiveAt reports whether the injection's window covers nowMs.
func (inj Injection) ActiveAt(now int64) bool {
	if now < inj.Start {
		return false
	}
	if inj.Duration <= 0 {
		return true
	}
	return now < inj.Start+inj.Duration
}

// Target is a component that can receive fault knobs. Implemented by
// queue-style components whose knob setters are deterministic given the
// knob state.
type Target interface {
	// ApplyDrop sets a drop probability on the target (0 = off).
	ApplyDrop(rate float64)
	// ApplyDelay sets a fixed per-task delay on the target (0 = off).
	ApplyDelay(ms int64)
	// ApplyTimeout sets a residency deadline on the target (0 = off).
	ApplyTimeout(ms int64)
	// TargetName identifies the component (diagnostics).
	TargetName() string
}

// Injector applies a list of Injections to one Target at pump time.
// An Injector with no active injection behaves identically to no Injector
// (zero draws, zero change).
type Injector struct {
	sim    *core.Simulation
	target Target
	active []Injection
}

// NewInjector creates an Injector over sim applying injections to target.
func NewInjector(sim *core.Simulation, target Target, injections ...Injection) *Injector {
	if sim == nil {
		panic("faults: NewInjector requires a non-nil simulation")
	}
	if target == nil {
		panic("faults: NewInjector requires a non-nil target")
	}
	return &Injector{sim: sim, target: target, active: injections}
}

// TargetName returns the target's name.
func (i *Injector) TargetName() string { return i.target.TargetName() }

// Apply applies the active injections at the current sim time. Call at
// each pump boundary. Deterministic: for each kind, the LAST active
// injection wins; a kind with no active window is set to off.
func (i *Injector) Apply() {
	now := i.sim.Now().Int64()
	var drop, delay, timeout bool
	var dropRate, delayMs, timeoutMs float64
	for _, inj := range i.active {
		if !inj.ActiveAt(now) {
			continue
		}
		switch inj.Kind {
		case KindDrop:
			drop = true
			dropRate = inj.Rate
		case KindDelay:
			delay = true
			delayMs = inj.Rate
		case KindTimeout:
			timeout = true
			timeoutMs = inj.Rate
		}
	}
	// Apply to the target (idempotent: setting 0 = off).
	if drop {
		i.target.ApplyDrop(dropRate)
	} else {
		i.target.ApplyDrop(0)
	}
	if delay {
		i.target.ApplyDelay(int64(delayMs))
	} else {
		i.target.ApplyDelay(0)
	}
	if timeout {
		i.target.ApplyTimeout(int64(timeoutMs))
	} else {
		i.target.ApplyTimeout(0)
	}
}

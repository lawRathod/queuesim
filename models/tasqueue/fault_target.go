package tasqueue

import (
	"github.com/lawRathod/queuesim/models/tasqueue/faults"
)

// applyFaults is the pump-time windowed-fault entry point: it delegates to
// the faults subpackage, which applies the active window knobs through this
// runtime's Target adapter. Called once per pump tick (tick, step 0). An
// empty Faults list early-returns — zero work and byte-identical to a
// config with no faults (TestFaultsInactiveZeroDraw).
func (rt *runtime) applyFaults(nowMs int64) {
	faults.Apply(rt, rt.cfg.Faults, nowMs)
}

// The faults subpackage applies every knob through the Target interface;
// these adapter methods forward to the broker queue and the runtime's
// effective-knob fields, so the subpackage never touches the model's
// unexported internals. They are one-line forwards — the semantics (and the
// per-tick call order) are unchanged from the pre-subpackage appliers.
var _ faults.Target = (*runtime)(nil)

// SetDropRate forwards the drain-time delivery drop rate to the broker
// queue.
func (rt *runtime) SetDropRate(rate float64) { rt.q.SetDropRate(rate) }

// SetEffectiveFailP stores the effective per-attempt handler failure
// probability read by complete.
func (rt *runtime) SetEffectiveFailP(p float64) { rt.effFailP = p }

// SetEffectiveTimeoutMs stores the effective attempt timeout deadline read
// by take.
func (rt *runtime) SetEffectiveTimeoutMs(ms int64) { rt.effTimeoutMs = ms }

// DropBaseline returns the static DropRate the appliers restore when no
// KindDrop window is active.
func (rt *runtime) DropBaseline() float64 { return rt.cfg.DropRate }

// FailPBaseline returns the static FailP the appliers restore when no
// KindFailure window is active.
func (rt *runtime) FailPBaseline() float64 { return rt.cfg.FailP }

// TimeoutBaseline returns the static TimeoutMs the appliers restore when no
// KindTimeout window is active.
func (rt *runtime) TimeoutBaseline() int64 { return rt.cfg.TimeoutMs }

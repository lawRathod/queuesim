package core

// FrontierEventKind classifies a frontier entry for traceability and
// diagnostics (ADR-014). It does NOT affect scheduler ordering —
// ordering is governed by `(at, seq)` per ADR-006.
type FrontierEventKind uint8

const (
	// KindCallback is the default for events scheduled via ScheduleAt / ScheduleIn.
	KindCallback FrontierEventKind = iota
	// KindTask is for events scheduled via the core/task package or
	// ScheduleAtEx with opts.Kind = KindTask. Used by the trace layer
	// to distinguish task cancellations from plain callback fires.
	KindTask
)

// ScheduleOptions is the optional per-event metadata accepted by
// ScheduleAtEx and ScheduleInEx. Plain ScheduleAt / ScheduleIn use the
// zero value (KindCallback, auto-assigned TaskID, empty ComponentID).
//
// All fields are advisory for tracing/diagnostics EXCEPT TaskID, which
// is the cancellation identity. Pass a non-zero TaskID when re-scheduling
// a Retry/Periodic chain so that Cancel on the original handle cancels
// the whole chain (DesCartes parity).
//
// Contract: at most one event with a given TaskID may be in the heap at
// any time. Re-scheduling a chain under the same TaskID is supported
// only from within the chain's own closure body (i.e., during its fire).
type ScheduleOptions struct {
	// TaskID is the cancellation identity. If zero, a new TaskID is
	// minted from the scheduler's seq counter (ADR-006). If non-zero,
	// the caller asserts they own this TaskID (e.g., a Retry chain
	// re-using its parent handle's ID).
	TaskID TaskID

	// Kind classifies the event for tracing. Defaults to KindCallback.
	Kind FrontierEventKind

	// ComponentID is an opaque string tag (e.g., a component name) for
	// diagnostics. No structure is enforced; empty by default.
	ComponentID string

	// Name is an optional breadcrumb for trace logging at schedule
	// time. Go has no equivalent of Rust's std::any::type_name::<E>(),
	// so callers may pass a human-readable name for diagnostics.
	Name string
}

package faults

import "github.com/lawRathod/queuesim/model"

// Spec is the fault-relevant projection of the temporal model's Config,
// built by the runtime on each delegation (fault_target.go) — the config
// fields the fault application reads directly. The model owns Config; this
// package never imports it. Traceability role: Fault-control plumbing
// (taxonomy in models/temporal/doc.go) — Spec carries the test knobs
// Apply accumulates, not server pins.
type Spec struct {
	// Vector is the active decision vector (nil = none). When several
	// members are active at a tick, Apply accumulates max-rate for the
	// drop kinds (add_drop/wf_drop/poll_drop) and last-wins for the
	// scalar kinds.
	Vector *model.DecisionVector

	// FaultStartMs and FaultEndMs are the default activity-failure window
	// (the config fallback when no vector overrides it).
	FaultStartMs int64
	FaultEndMs   int64 // 0 = open-ended

	// CascadeOn enables cascade mixed-population mode: bad actors (workflow
	// ID hash < CascadeBadFraction) fail during the default fault window;
	// healthy workflows never fail.
	CascadeOn bool
	// CascadeBadFraction is the bad-actor fraction in cascade mode.
	CascadeBadFraction float64

	// DispatchRate is the token-bucket dispatch rate on both matching
	// queues (0 = unlimited).
	DispatchRate float64
	// DispatchBurst is the token-bucket capacity.
	DispatchBurst int

	// ActivityS2STimeoutMs is the matching-act MaxWaitMs default — the
	// queue's constructed value, applied when no s2s_timeout member is
	// active.
	ActivityS2STimeoutMs int64
}

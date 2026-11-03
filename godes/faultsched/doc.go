// Package faultsched provides a shared, stateless, TIME-KEYED fault
// schedule (ADR-100) consumed by BOTH legs of the model ↔ synctest-bubble
// validation loop:
//
//   - the queue-centric Temporal model (GoDES/models/temporal) samples it
//     once per pump tick (~60k queries per run), and
//   - the real Temporal server in the synctest bubble (GoDES/temporal)
//     samples it per RPC arrival (hundreds of queries per run).
//
// The schedule is NOT a stateful PRNG stream: a stream consumed at
// different rates on the two legs would diverge from the first decision.
// Instead State is a pure function of (seed, elapsedMs): the same
// (seed, elapsedMs) returns the same active-fault answer regardless of who
// asks or how often, so both legs inject identical fault schedules BY
// CONSTRUCTION. Per-event coin flips stay leg-local on each side's own
// RNG — cross-leg comparison targets the RATE SCHEDULE, never coin
// outcomes.
//
// Windows are half-open [StartMs, EndMs); EndMs == 0 is open-ended from
// StartMs. NewSchedule validates and deterministically sorts its windows
// by (StartMs, Kind); State returns the active faults in that sorted order
// (no map iteration on any decision path). Overlapping windows of the SAME
// kind resolve to the LAST active by sorted order; different kinds coexist.
//
// A nil Schedule (NewSchedule with nil/empty windows, FromTrace with a
// nil/empty trace) is the byte-neutral NO-SCHEDULE state: nil means "no
// schedule" at every call site, and a nil Schedule or nil Source answers no
// faults for every query.
//
// Determinism contract: single-goroutine, no wall clock, no goroutines, no
// map iteration on decision paths, no hidden state (seed is a parameter of
// State, never stored on the Schedule). Validation rejects NaN/±Inf rates.
//
// The kind vocabulary is the 12 existing model kinds as PLAIN STRINGS
// matching model.VectorKind values (no model import — this package is a
// stdlib-only leaf) plus three RESERVED kinds (persistence_fail,
// duplicate, partition) that validate and round-trip but have no leg
// behavior yet (ADR-100: a schedule naming a reserved kind must fail
// loudly on the leg that meets one, never silently no-op).
package faultsched

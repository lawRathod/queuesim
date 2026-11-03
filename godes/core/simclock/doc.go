// Package simclock provides a unified clock abstraction for deterministic
// discrete-event simulation in GoDES.
//
// The Clock interface is the single source of truth for simulation time.
// Any system that uses this interface instead of the standard library's
// time.Now, context.WithDeadline, or time.After can run deterministically
// inside a GoDES simulation.
//
// Two implementations are provided:
//
//   - WallClock delegates to the real time and context packages. Use this
//     for production code that runs outside a simulation.
//
//   - SimClock uses the DES scheduler for all time operations. Use this
//     for simulated code that must be deterministic and replayable.
//
// The package also provides Timer and Ticker types that mirror the standard
// library's time.Timer and time.Ticker, and a deadlineContext that implements
// context.Context with sim-time-driven deadline checks (extracted from the
// tasqueue package per ADR-042).
//
// See ADR-043 for the full contract and rationale.
package simclock

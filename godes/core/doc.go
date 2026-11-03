// Package core provides the deterministic Phase 1 simulation engine primitives.
//
// It contains virtual time arithmetic, event scheduling, deterministic tie-breaking
// policies, and simulation-scoped pseudo-random sources. The package is designed
// to be replayable: identical inputs and seeds must yield identical execution traces.
//
// Sub-packages:
//
//	component — typed component registry for scenario building
package core

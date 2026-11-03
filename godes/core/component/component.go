// Package component provides a typed component registry for GoDES scenarios.
//
// Components receive a *core.Simulation reference during event processing for
// scheduling subsequent events. Components must NOT call Run() or Step() on the
// simulation from within their ProcessEvent handler.
package component

import "github.com/lawRathod/queuesim/godes/core"

// Key is a typed handle for a registered component.
// The zero value Key[T]{} is safe to use and will never collide with
// a real component handle, since Add assigns IDs starting from 1.
type Key[T any] struct {
	id uint64
}

// Component is the interface all components must implement.
// ProcessEvent receives the simulation for scheduling; do NOT call Run/Step.
type Component[T any] interface {
	ProcessEvent(self Key[T], ev T, sim *core.Simulation)
}

// Registry manages typed component registration and dispatch.
//
// Registry is not safe for concurrent access. The DES engine is single-goroutine
// and all ProcessEvent callbacks execute sequentially on the simulation's
// goroutine, so no synchronization primitives are needed.
type Registry struct {
	next  uint64
	sim   *core.Simulation
	comps map[uint64]any
}

// NewRegistry creates a new component registry bound to the given simulation.
func NewRegistry(sim *core.Simulation) *Registry {
	return &Registry{sim: sim, comps: make(map[uint64]any)}
}

// Add registers a component and returns a typed Key for dispatching events to it.
func Add[T any](r *Registry, c Component[T]) Key[T] {
	r.next++
	id := r.next
	r.comps[id] = c
	return Key[T]{id: id}
}

// Get retrieves a registered component by its typed Key.
// Returns nil if the key does not correspond to a registered component
// or if the stored component's type does not match T.
func Get[T any](r *Registry, key Key[T]) Component[T] {
	if v, ok := r.comps[key.id]; ok {
		c, ok := v.(Component[T])
		if !ok {
			return nil
		}
		return c
	}
	return nil
}

// Dispatch delivers an event to the component identified by the given key.
// If the key is not registered or the type does not match, Dispatch is a no-op.
func Dispatch[T any](r *Registry, key Key[T], ev T) {
	if r.sim == nil {
		panic("component: Dispatch called on Registry with nil sim; use NewRegistry to create a Registry")
	}
	if c := Get(r, key); c != nil {
		c.ProcessEvent(key, ev, r.sim)
	}
}

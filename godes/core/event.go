package core

// Event is the callback scheduled for execution at a specific simulation time.
//
// Phase 1 uses a callback-only scheduling model; no separate event object lifecycle
// is defined at this layer. Callers schedule work by passing an Event to ScheduleAt
// or ScheduleIn.
type Event func(*Simulation)

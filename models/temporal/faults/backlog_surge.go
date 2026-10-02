package faults

// KindBacklogSurge has NO pump-time operation: Apply's switch case is a
// deliberate no-op (see faults.go). The surge is a Run-time scheduling
// concern — a secondary frontend burst at each member's StartMs — handled
// by scheduleBacklogSurge in the temporal model
// (models/temporal/load.go), called once from the runtime's start.
// Fault-control (taxonomy in models/temporal/doc.go): a test knob at the
// frontend-burst chokepoint, not a server pin.

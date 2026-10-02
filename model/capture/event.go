package capture

// Event is one observed mechanism transition. From/To are the model's own
// component names; Kind is a model-chosen transition category. Optional
// fields (task identity, class, count) use omitempty so a zero-valued
// field is absent from the JSON line. Events are deterministic: Seq is the
// single-goroutine sim order and TsMs is sim time in milliseconds — no
// wall-clock, no goroutine interleaving enters the record.
type Event struct {
	// Seq is the recorder-global monotonic sequence number. The
	// single-goroutine sim order IS the sequence order.
	Seq uint64 `json:"seq"`
	// TsMs is the sim timestamp in milliseconds when the transition was
	// recorded.
	TsMs int64 `json:"ts_ms,omitempty"`
	// From is the source component name (model-chosen).
	From string `json:"from,omitempty"`
	// To is the destination component name (model-chosen).
	To string `json:"to,omitempty"`
	// Kind is the transition category (model-chosen, e.g. "workflow_start",
	// "wf_add", "dispatch", "act_retry").
	Kind string `json:"kind,omitempty"`
	// TaskID is the workflow/activity/task identity when the transition can
	// be joined to one.
	TaskID string `json:"task_id,omitempty"`
	// Attempt is the task attempt number when known.
	Attempt int `json:"attempt,omitempty"`
	// Class is the task class (0 = workflow start, 1 = activity, 2 =
	// workflow completion) when known.
	Class int `json:"class,omitempty"`
	// Count is a transition quantity (e.g. spooled batch size) when the
	// transition is a compound/aggregate record.
	Count int `json:"count,omitempty"`
}

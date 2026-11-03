package core

import (
	"log/slog"
	"math"
	mathrand "math/rand/v2"
	"sync"
)

// FrontierPolicy selects among equal-time events during Step().
type FrontierPolicy interface {
	Choose(at SimTime, frontier []FrontierEvent) int
}

// FrontierEvent describes a single event in a same-time frontier for policy selection.
type FrontierEvent struct {
	Seq         uint64
	Kind        FrontierEventKind
	ComponentID string
	TaskID      TaskID
}

// FIFOPolicy implements deterministic tie-breaking among same-time events.
// Ordering: ComponentID, then TaskID, then Seq (tertiary, for events sharing
// a key — e.g. chain re-schedules). Seq reflects lock-acquisition order for
// concurrent schedulers, so it is NEVER the primary key (ADR-058 Layer 2);
// concurrent same-time schedulers must disambiguate via ComponentID/TaskID.
type FIFOPolicy struct{}

func (FIFOPolicy) Choose(at SimTime, frontier []FrontierEvent) int {
	minIdx := 0
	for i := 1; i < len(frontier); i++ {
		if lessByKey(frontier[i], frontier[minIdx]) {
			minIdx = i
		}
	}
	return minIdx
}

// lessByKey orders frontier events by (ComponentID, TaskID, Seq).
func lessByKey(a, b FrontierEvent) bool {
	if a.ComponentID != b.ComponentID {
		return a.ComponentID < b.ComponentID
	}
	if a.TaskID != b.TaskID {
		return a.TaskID < b.TaskID
	}
	return a.Seq < b.Seq
}

// SeededRandomPolicy selects randomly among same-time events.
// Deterministic given a fixed seed.
type SeededRandomPolicy struct {
	Seed uint64
	rng  *mathrand.Rand
}

func (p *SeededRandomPolicy) Choose(at SimTime, frontier []FrontierEvent) int {
	if len(frontier) == 0 {
		return 0
	}
	if p.rng == nil {
		p.rng = mathrand.New(mathrand.NewPCG(p.Seed, p.Seed^0x9E3779B97F4A7C15))
	}
	return p.rng.IntN(len(frontier))
}

// Metadata captures deterministic run settings.
type Metadata struct {
	Seed uint64
}

// Simulation is the minimal deterministic core engine loop for Phase 1.
// Thread-safe (ADR-058 Layer 1): all sim ops lock s.mu; event handlers run
// outside the lock (they may block on transport reads).
type Simulation struct {
	mu             sync.Mutex
	now            SimTime
	seq            uint64
	seed           uint64
	root           RNG
	substreams     map[string]RNG
	queue          *eventHeap
	policy         FrontierPolicy
	cancelledTasks map[TaskID]struct{}
	log            *slog.Logger
}

// NewSimulation builds a simulation with a deterministic stdlib-backed RNG.
func NewSimulation(seed int64, policy FrontierPolicy) (*Simulation, error) {
	if policy == nil {
		policy = FIFOPolicy{}
	}
	s := uint64(seed)
	return &Simulation{
		now:            0,
		seq:            0,
		seed:           s,
		root:           newStdlibRNG(s),
		substreams:     make(map[string]RNG),
		queue:          newEventHeap(),
		policy:         policy,
		cancelledTasks: make(map[TaskID]struct{}),
	}, nil
}

// Metadata returns deterministic run metadata.
func (s *Simulation) Metadata() Metadata {
	return Metadata{Seed: s.seed}
}

// Now returns current virtual simulation time.
func (s *Simulation) Now() SimTime {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.now < 0 {
		return 0
	}
	return s.now
}

// RNG returns the simulation-owned root random source. Draws are serialized
// through the simulation mutex (ADR-058 Layer 1).
func (s *Simulation) RNG() RNG {
	return &lockedRNG{mu: &s.mu, inner: s.root}
}

// Substream returns a deterministic, stable-named substream RNG. Draws are
// serialized through the simulation mutex; draw ORDER remains caller-
// dependent (fixed in Layer 3, ADR-058).
func (s *Simulation) Substream(name string) RNG {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.substreams[name]; ok {
		return &lockedRNG{mu: &s.mu, inner: r}
	}
	seed := deriveSubstreamSeed(s.seed, name)
	r := newStdlibRNG(seed)
	s.substreams[name] = r
	return &lockedRNG{mu: &s.mu, inner: r}
}

// SetLogger attaches a structured logger to the simulation for breadcrumb
// logging at schedule time (when ScheduleOptions.Name is set).
func (s *Simulation) SetLogger(logger *slog.Logger) {
	s.log = logger
}

// ScheduleAt schedules ev at absolute simulation time and returns a
// TaskHandle that can be passed to Cancel. Negative at is normalized to zero.
// Source-compatible: callers that previously ignored the return value
// continue to compile (ADR-036).
func (s *Simulation) ScheduleAt(at SimTime, ev Event) TaskHandle {
	return s.ScheduleAtEx(at, ev, ScheduleOptions{})
}

// ScheduleIn schedules ev relative to current simulation time using
// saturating arithmetic and returns a TaskHandle.
func (s *Simulation) ScheduleIn(delta int64, ev Event) TaskHandle {
	at := s.Now().Add(delta)
	return s.ScheduleAtEx(at, ev, ScheduleOptions{})
}

// ScheduleAtEx schedules ev at absolute simulation time with optional
// metadata (TaskID override, Kind, ComponentID, Name).
//
// Kind defaults:
//   - If opts.Kind is zero and opts.TaskID is zero: KindCallback
//     (plain scheduled event, standard behavior).
//   - If opts.Kind is zero and opts.TaskID is non-zero: KindTask
//     (chain reschedule convention — a Retry/Periodic chain re-scheduling
//     itself under the same TaskID for whole-chain cancellation).
//   - If opts.Kind is set explicitly, it is used as-is regardless of TaskID.
//
// TaskID: If zero, a new TaskID is minted from the scheduler's seq
// counter. If non-zero, the caller asserts they own this ID (e.g., a
// Retry chain re-using its parent's ID). Contract: at most one event
// with a given TaskID may be in the heap at any time (ADR-044).
func (s *Simulation) ScheduleAtEx(at SimTime, ev Event, opts ScheduleOptions) TaskHandle {
	if ev == nil {
		return TaskHandle{}
	}
	if at < 0 {
		at = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	taskID := opts.TaskID
	if taskID == 0 {
		taskID = TaskID(s.seq)
	}
	if opts.Kind == 0 && opts.TaskID == 0 {
		opts.Kind = KindCallback
	} else if opts.Kind == 0 {
		opts.Kind = KindTask
	}
	s.queue.push(scheduledEvent{
		at:     at,
		seq:    s.seq,
		taskID: taskID,
		kind:   opts.Kind,
		compID: opts.ComponentID,
		name:   opts.Name,
		fn:     ev,
	})
	if s.log != nil && opts.Name != "" {
		s.log.Debug("scheduled",
			"name", opts.Name,
			"task_id", uint64(taskID),
			"kind", uint8(opts.Kind),
			"at", at.Int64(),
			"seq", s.seq,
		)
	}
	return TaskHandle{id: taskID}
}

// ScheduleInEx schedules ev relative to current simulation time with
// optional metadata (see ScheduleAtEx).
func (s *Simulation) ScheduleInEx(delta int64, ev Event, opts ScheduleOptions) TaskHandle {
	at := s.Now().Add(delta)
	return s.ScheduleAtEx(at, ev, opts)
}

// popNextCancelable pops the next non-cancelled event from the queue,
// skipping events scheduled after maxAt. The maxAt parameter prevents
// cancelled events from causing the pop loop to overshoot the target
// time (see ADR-044 and the TestRunUntilDoesNotAdvanceBeyondTarget test).
// Caller must hold s.mu (ADR-058 Layer 1).
func (s *Simulation) popNextCancelable(maxAt SimTime) (scheduledEvent, bool) {
	for {
		peek, ok := s.queue.peek()
		if !ok || peek.at > maxAt {
			return scheduledEvent{}, false
		}
		ev, _ := s.queue.pop()
		if _, cancelled := s.cancelledTasks[ev.taskID]; cancelled {
			delete(s.cancelledTasks, ev.taskID)
			continue
		}
		return ev, true
	}
}

// stepUntil executes at most one pending event with time <= maxAt.
// Returns false when no eligible event exists. Queue work happens under
// the lock in prepareStep; the handler runs OUTSIDE the lock (it may
// block on transport reads; ADR-058 Layer 1).
func (s *Simulation) stepUntil(maxAt SimTime) bool {
	chosen, ok := s.prepareStep(maxAt)
	if !ok {
		return false
	}
	chosen.fn(s)
	return true
}

// prepareStep does the locked queue work of stepUntil under a single lock
// hold: pop, frontier formation, policy choice, re-push, now clamp. Returns
// the chosen event with the lock released.
func (s *Simulation) prepareStep(maxAt SimTime) (scheduledEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first, ok := s.popNextCancelable(maxAt)
	if !ok {
		return scheduledEvent{}, false
	}

	// Form frontier: all non-cancelled events at the same time.
	frontier := []scheduledEvent{first}
	for {
		peek, ok := s.queue.peek()
		if !ok || peek.at != first.at {
			break
		}
		ev, _ := s.queue.pop()
		if _, cancelled := s.cancelledTasks[ev.taskID]; cancelled {
			delete(s.cancelledTasks, ev.taskID)
			continue
		}
		frontier = append(frontier, ev)
	}

	// Let the policy choose.
	fEvents := make([]FrontierEvent, len(frontier))
	for i, ev := range frontier {
		fEvents[i] = FrontierEvent{
			Seq:         ev.seq,
			Kind:        ev.kind,
			ComponentID: ev.compID,
			TaskID:      ev.taskID,
		}
	}
	// ADR-058: identical (ComponentID, TaskID) at the same sim time is a
	// scheduling ambiguity — the tie is then decided by seq, which is
	// lock-acquisition-ordered for concurrent schedulers (nondeterministic).
	// Detected loudly; flip to panic once the suite proves collision-free.
	for i := 0; i < len(fEvents); i++ {
		for j := i + 1; j < len(fEvents); j++ {
			if fEvents[i].ComponentID == fEvents[j].ComponentID && fEvents[i].TaskID == fEvents[j].TaskID {
				if s.log != nil {
					s.log.Warn("ambiguous same-time event key",
						"at", first.at.Int64(), "comp_id", fEvents[i].ComponentID, "task_id", uint64(fEvents[i].TaskID), "seq_a", fEvents[i].Seq, "seq_b", fEvents[j].Seq)
				} else {
					slog.Default().Warn("ambiguous same-time event key",
						"at", first.at.Int64(), "comp_id", fEvents[i].ComponentID, "task_id", uint64(fEvents[i].TaskID), "seq_a", fEvents[i].Seq, "seq_b", fEvents[j].Seq)
				}
			}
		}
	}
	chosenIdx := s.policy.Choose(first.at, fEvents)
	if chosenIdx < 0 || chosenIdx >= len(frontier) {
		chosenIdx = 0
	}
	chosen := frontier[chosenIdx]

	// Re-push unchosen events.
	for i, ev := range frontier {
		if i != chosenIdx {
			s.queue.push(ev)
		}
	}

	// Clamp to now to maintain monotonicity.
	if chosen.at < s.now {
		chosen.at = s.now
	}
	s.now = chosen.at
	return chosen, true
}

// Step executes at most one pending event. Returns false when queue is empty.
func (s *Simulation) Step() bool {
	return s.stepUntil(math.MaxInt64)
}

// Run executes events until queue exhaustion.
func (s *Simulation) Run() {
	for s.Step() {
	}
}

// RunUntil executes events with scheduled time <= target. Uses the
// frontier policy (via stepUntil) so that same-time events during a bounded
// run are arbitrated identically to Step.
func (s *Simulation) RunUntil(target SimTime) {
	if target < 0 {
		target = 0
	}
	for {
		s.mu.Lock()
		peek, ok := s.queue.peek()
		s.mu.Unlock()
		if !ok || peek.at > target {
			return
		}
		if !s.stepUntil(target) {
			return
		}
	}
}

// Advance moves the sim clock forward by ticks sim-time units, firing
// any events scheduled in (now, now+ticks]. This is the "the handler
// does work that takes N sim ticks" primitive — see ADR-042. The
// sim remains pure: no wall-clock dependency is introduced.
func (s *Simulation) Advance(ticks int64) {
	if ticks <= 0 {
		return
	}
	target := s.Now() + SimTime(ticks)
	s.ScheduleAt(target, func(s *Simulation) {}) //nolint:staticcheck // SA4006: return value intentionally discarded
	s.RunUntil(target)
}

// ClampToMaxTime normalizes large absolute values to valid SimTime range.
func ClampToMaxTime(v int64) SimTime {
	if v <= 0 {
		return 0
	}
	if v >= math.MaxInt64 {
		return SimTime(math.MaxInt64)
	}
	return SimTime(v)
}

// Cancel marks the event referenced by h as cancelled. Returns true if
// the task was pending, false otherwise (already fired, double-cancel,
// or h is the zero TaskHandle).
//
// Semantics (DesCartes parity):
//   - Sets a tombstone in the side-channel cancelledTasks set.
//   - The pop loop in Step / popNextCancelable consults the tombstone
//     and SKIPS cancelled events WITHOUT advancing sim time. The
//     tombstone is consumed on first skip.
//   - If a chain re-schedules under the same TaskID after Cancel, the
//     new heap entry will be skipped on its next pop (because the
//     tombstone is still present — it is only consumed on actual skip).
//   - Double-cancel returns false: the tombstone was consumed by the
//     first skip, or the task had already fired.
//
// Cancel is a thread-safe op (ADR-058 Layer 1); single-goroutine callers
// see identical behavior (ADR-003).
func (s *Simulation) Cancel(h TaskHandle) bool {
	if h.id == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.cancelledTasks[h.id]; exists {
		return false
	}
	if !s.queue.hasTaskID(h.id) {
		return false
	}
	s.cancelledTasks[h.id] = struct{}{}
	return true
}

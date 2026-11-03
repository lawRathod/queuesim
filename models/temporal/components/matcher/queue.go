package matcher

import (
	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/core/dists"
)

// TaskClass distinguishes the task kinds that traverse the Temporal
// lifecycle. Only the three matching-boundary classes are modeled: timer
// and transfer events are handled inside the history shard queues, not as
// task classes (the components-era TaskTimerFired/TaskTransfer were
// proof-of-placement and are gone).
type TaskClass int

const (
	// TaskWfStart is the initial workflow-task.
	TaskWfStart TaskClass = iota
	// TaskActivity is an activity-task.
	TaskActivity
	// TaskWfComplete is the post-activity workflow-task.
	TaskWfComplete
)

// Task is a unit of work flowing through the temporal network.
type Task struct {
	ID          uint64
	Attempt     int32
	Class       TaskClass
	BornMs      int64
	Depth       int
	ParentID    uint64
	Rescheduled bool
}

// TaskExpireStage mirrors the server's staged task expiry. The four slots
// are: the none sentinel (0), expiry at spool (add — retained for array-
// layout compatibility but NEVER incremented: the model removed the invented
// add-time expiry branch; residency expiry is observed only at dispatch),
// expiry at read (the persistence-read stage — reserved: the model's backlog
// is memory-resident, so no task expires there), and expiry at poll
// (dispatch). The model exercises Poll only; Read is kept so the expiration
// array layout matches the server's staged model.
type TaskExpireStage int

const (
	// ExpireNone is the no-expiry sentinel (index 0 of the expiry array).
	ExpireNone TaskExpireStage = iota
	// ExpireAtSpool is expiry at add time — retained for array-layout
	// compatibility but never incremented (no add-time expiry branch).
	ExpireAtSpool
	// ExpireAtRead is expiry at the persistence-read stage — reserved.
	ExpireAtRead
	// ExpireAtPoll is expiry at dispatch time (waited out its deadline).
	ExpireAtPoll
)

// parkedPoller is one long-polling worker registered with the queue.
type parkedPoller struct {
	id       uint64
	expiryMs int64
	// parkedAtMs records when the poller parked. It is currently write-only:
	// the old poller-age sync check was removed (an already-parked poller is
	// always eligible for sync-match), so nothing reads it. Retained so the
	// parking record stays complete — do NOT reintroduce an age check here;
	// eligibility is governed by the Offer deadline on the add side.
	parkedAtMs int64
	wake       core.TaskHandle
	onTake     func(Task)
	consumed   bool
}

// ParkedPoller is the exported alias for the parked poller token.
type ParkedPoller = parkedPoller

// MatchQueue is a mechanism-faithful matching queue (one task queue per class).
type MatchQueue struct {
	sim *core.Simulation

	name   string
	prefix string
	// capacity is retained for API compatibility but no longer enforced:
	// matching is persistence-backed (unbounded), so Add/SpoolOnly never
	// reject and rejected stays zero.
	capacity  int
	maxWaitMs int64
	batch     int

	syncMatchWindowMs int64

	// backlogGateMs (Mechanism A — the isBacklogNegligible gate) is the
	// head-of-backlog AGE threshold in ms at which the foreground sync-match
	// fast path is disabled and adds route to the local spooled backlog
	// (matching the real server's matcher.go:109-116 gate whose threshold is
	// getBacklogAge() < 5s, dynamicconfig/constants.go:1277-1279). 0 =
	// disabled (no gate — the byte-neutral default).
	backlogGateMs int64
	// forwardReliable (Mechanism B — window-mediated forwarding) is whether
	// this partition can offload tasks to a root poller. When false (the
	// short/no sync-match window case — TrySyncMatch builds an instantly
	// expired child context that kills forwarding), the sync-match fast path
	// is unavailable: adds stay LOCAL in the backlog and accumulate, which is
	// what drives the backlog past the age gate (Mechanism A). Default true
	// = forwarding works (the healthy window-200 behavior).
	forwardReliable bool

	bucket    *TokenBucket
	rng       dists.Source
	rngPrefix string

	backlog []Task
	parked  []*parkedPoller
	nextID  uint64

	spooled    uint64
	synced     uint64
	rejected   uint64 // always zero: matching is persistence-backed
	dispatched uint64
	expired    [4]uint64 // ExpireAtPoll only; ExpireAtSpool slot stays zero

	spoolDropRate float64
	dropRng       dists.Source
	dropRngPrefix string
	dispatchDelay int64
	droppedDrop   uint64
}

// NewMatchQueue builds a queue bound to sim.
func NewMatchQueue(sim *core.Simulation, name string, capacity int, maxWaitMs, batch int64, rate, burst float64, syncMatchMs ...int64) *MatchQueue {
	return NewMatchQueueWithPrefix(sim, "", name, capacity, maxWaitMs, batch, rate, burst, syncMatchMs...)
}

// NewMatchQueueWithPrefix builds a queue with a hierarchical prefix.
func NewMatchQueueWithPrefix(sim *core.Simulation, prefix, name string, capacity int, maxWaitMs, batch int64, rate, burst float64, syncMatchMs ...int64) *MatchQueue {
	q := &MatchQueue{
		sim:             sim,
		name:            name,
		prefix:          prefix,
		capacity:        capacity,
		maxWaitMs:       maxWaitMs,
		bucket:          NewTokenBucket(rate, burst),
		forwardReliable: true, // healthy default: partition can forward to root
	}
	if len(syncMatchMs) > 0 {
		q.syncMatchWindowMs = syncMatchMs[0]
	}
	if batch <= 0 {
		batch = 1
	}
	q.batch = int(batch)
	if rate > 0 {
		q.rng = src(sim, prefix, "match."+name)
		q.rngPrefix = prefix
	}
	return q
}

// Len returns the backlog depth.
func (q *MatchQueue) Len() int { return len(q.backlog) }

// Parked returns the number of currently long-polling workers.
func (q *MatchQueue) Parked() int { return len(q.parked) }

// Park registers a long-polling worker.
func (q *MatchQueue) Park(expiryMs int64, onTake func(Task), onExpire ...func()) *ParkedPoller {
	p := &parkedPoller{id: q.nextID, expiryMs: expiryMs, parkedAtMs: q.sim.Now().Int64(), onTake: onTake}
	q.nextID++
	q.parked = append(q.parked, p)
	p.wake = q.sim.ScheduleAt(core.SimTime(expiryMs), func(*core.Simulation) {
		if p.consumed {
			return
		}
		q.remove(p)
		if len(onExpire) > 0 && onExpire[0] != nil {
			onExpire[0]()
		}
	})
	return p
}

func (q *MatchQueue) remove(p *ParkedPoller) {
	for i, e := range q.parked {
		if e == p {
			q.parked = append(q.parked[:i], q.parked[i+1:]...)
			return
		}
	}
}

// Add offers a task to the matching queue (the server's AddTask / OfferTask
// path — matcher.go Offer, MatchingSyncMatchWaitDuration).
//
//   - Gate closed (backlog-age gate tripped, or forwarding unavailable via
//     syncAvailable) → spool immediately to the local backlog (no sync
//     attempt, no wait).
//   - Parked poller available → sync-match inline: consume one dispatch
//     token BEFORE the handoff (sync Consume before match); a throttled
//     offer (Consume refused) spools instead of sync-matching. Poller age
//     is NOT consulted — an already-parked poller is always eligible (the
//     old poller-age check is removed).
//   - No parked poller → spool immediately (immediate-spool semantics; the
//     task stays in the counted backlog and is served via TryDispatch).
//
// There is deliberately NO Offer-wait here: the Offer-deadline RENDEZVOUS
// that would justify waiting up to SyncMatchWindowMs for a late-parking
// poller is unimplemented (the server Offer would rendezvous; waiting then
// spooling gains nothing), and holding the task in a ScheduleAt closure
// makes it invisible to the runtime's backlog-depth / DrainMs measurement
// (a never-draining storm would transiently read empty). Immediate spool
// keeps the task in the counted backlog and preserves the calibrated
// behavior.
//
// There is deliberately NO expiry-at-spool branch: residency expiry is
// observed only at dispatch (ExpireAtPoll in TryDispatch), matching the
// server where a schedule-to-start timeout fires while the task waits, not
// at add time. There is deliberately NO capacity reject: matching is
// persistence-backed (unbounded); the capacity field is retained for API
// compatibility and the rejected counter stays zero. accepted is therefore
// always true. Zero RNG draws on every path; no sim events.
func (q *MatchQueue) Add(t Task, now core.SimTime) (syncMatched, accepted bool) {
	if !q.syncAvailable(now) {
		q.backlog = append(q.backlog, t)
		q.spooled++
		return false, true
	}
	if len(q.parked) > 0 {
		if !q.bucket.Consume(now.Int64()) {
			q.backlog = append(q.backlog, t)
			q.spooled++
			return false, true
		}
		p := q.parked[0]
		q.parked = q.parked[1:]
		p.consumed = true
		q.sim.Cancel(p.wake)
		q.synced++
		q.dispatched++
		p.onTake(t)
		return true, true
	}
	// No parked poller: spool immediately (immediate-spool semantics). There
	// is deliberately NO offer-wait here: the Offer-deadline RENDEZVOUS that
	// would justify waiting up to SyncMatchWindowMs for a late-parking poller
	// is unimplemented (the server Offer would rendezvous; waiting then
	// spooling gains nothing), and holding the task in a ScheduleAt closure
	// makes it invisible to the runtime's backlog-depth / DrainMs measurement
	// (the never-draining storm would transiently read empty). Immediate spool
	// keeps the task in the counted backlog and preserves the calibrated
	// behavior.
	q.backlog = append(q.backlog, t)
	q.spooled++
	return false, true
}

// syncAvailable reports whether the sync-match fast path is usable at now.
// Mechanism B (forwardReliable) is the causal driver: a short/no window
// means the child cannot offload a forwarded task to the root, so the fast
// path is down. Mechanism A (backlog-age gate / isBacklogNegligible) closes
// the fast path once the local backlog has aged past the gate threshold — the
// gate's DESIGNED behavior (whether it latches in any given cell is
// cell-dependent; at the syncmatch_sweep calibrated aps it does NOT, see the
// scenario doc.go). Under the healthy defaults (forwarding reliable + gate
// threshold above the calm backlog age) syncAvailable is true and Add
// behaves exactly as before — byte-neutral.
func (q *MatchQueue) syncAvailable(now core.SimTime) bool {
	if !q.forwardReliable {
		return false
	}
	return !q.gateTripped(now)
}

// gateTripped is the isBacklogNegligible() analog: the gate is CLOSED when
// the oldest (head) backlog task has been resident AT LEAST backlogGateMs
// (matching the real server's isBacklogNegligible, which is false when
// getBacklogAge() >= 5s — matcher.go:109-116, dynamicconfig/constants.go:
// 1277-1279). 0 gate = never tripped.
func (q *MatchQueue) gateTripped(now core.SimTime) bool {
	if q.backlogGateMs <= 0 || len(q.backlog) == 0 {
		return false
	}
	return now.Int64()-q.backlog[0].BornMs >= q.backlogGateMs
}

// GateTripped is the exported read of the backlog-age gate (isBacklogNegligible
// analog, Mechanism A): true when the head-of-backlog age has reached the
// gate threshold. 0 gate / empty backlog → false. Pure read, zero RNG.
func (q *MatchQueue) GateTripped(now core.SimTime) bool { return q.gateTripped(now) }

// HeadAge returns the head-of-backlog age (ms) at now — how long the oldest
// backlogged task has been resident. 0 when the backlog is empty. Pure read,
// zero RNG.
func (q *MatchQueue) HeadAge(now core.SimTime) int64 {
	if len(q.backlog) == 0 {
		return 0
	}
	age := now.Int64() - q.backlog[0].BornMs
	if age < 0 {
		return 0
	}
	return age
}

// PopHead removes and returns the oldest backlogged task WITHOUT consulting
// pollers, the token bucket, or any drop/expiry RNG — the deterministic
// head-of-queue read used by the child partition's own local drain (drain-
// bound path). Returns ok=false when the backlog is empty.
func (q *MatchQueue) PopHead() (Task, bool) {
	if len(q.backlog) == 0 {
		return Task{}, false
	}
	t := q.backlog[0]
	q.backlog = q.backlog[1:]
	return t, true
}

// SetBacklogGate sets the backlog-age gate threshold (Mechanism A). 0 =
// disabled (the byte-neutral default: no gate). Negative values clamp to 0.
func (q *MatchQueue) SetBacklogGate(ms int64) {
	if ms < 0 {
		ms = 0
	}
	q.backlogGateMs = ms
}

// SetForwardReliable sets whether this partition can forward tasks to a root
// poller (Mechanism B). false = forwarding dead (short/no sync-match window)
// → adds stay local and accumulate; true = forwarding works (healthy window).
func (q *MatchQueue) SetForwardReliable(b bool) {
	q.forwardReliable = b
}

// TryDispatch hands one backlog task to a parked poller.
func (q *MatchQueue) TryDispatch(now core.SimTime) (Task, bool) {
	if len(q.parked) == 0 || len(q.backlog) == 0 {
		return Task{}, false
	}
	if !q.bucket.Consume(now.Int64()) {
		return Task{}, false
	}
	t := q.backlog[0]
	if q.maxWaitMs > 0 && now.Int64()-t.BornMs > q.maxWaitMs {
		q.backlog = q.backlog[1:]
		q.expired[ExpireAtPoll]++
		return t, true
	}
	if q.spoolDropRate > 0 && q.dropRng.Float64() < q.spoolDropRate {
		q.backlog = q.backlog[1:]
		q.droppedDrop++
		return t, true
	}
	q.backlog = q.backlog[1:]
	p := q.parked[0]
	q.parked = q.parked[1:]
	p.consumed = true
	q.sim.Cancel(p.wake)
	q.dispatched++
	if q.dispatchDelay > 0 {
		q.sim.ScheduleIn(q.dispatchDelay, func(*core.Simulation) { p.onTake(t) })
		return t, true
	}
	p.onTake(t)
	return t, true
}

// DispatchTask hands a task that already exists (e.g. from a per-worker
// sticky buffer) directly to a parked poller — the matching service's
// handoff for tasks that bypass the class backlog. It reuses the sync-match
// accounting (poller removed, wake canceled, dispatched++); returns false
// when no poller is parked. Never draws RNG.
func (q *MatchQueue) DispatchTask(t Task) bool {
	if len(q.parked) == 0 {
		return false
	}
	p := q.parked[0]
	q.parked = q.parked[1:]
	p.consumed = true
	q.sim.Cancel(p.wake)
	q.dispatched++
	p.onTake(t)
	return true
}

// ExpiredCount returns per-stage expiry counters.
func (q *MatchQueue) ExpiredCount() [4]uint64 { return q.expired }

// DroppedCount returns the total spool-time fault drops.
func (q *MatchQueue) DroppedCount() uint64 { return q.droppedDrop }

// SetMaxWaitMs sets the residency deadline.
func (q *MatchQueue) SetMaxWaitMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	q.maxWaitMs = ms
}

// SetSpoolDrop sets the spool-time drop probability.
func (q *MatchQueue) SetSpoolDrop(rate float64) {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	q.spoolDropRate = rate
	if rate > 0 && q.dropRng == nil {
		q.dropRng = src(q.sim, q.prefix, "match."+q.name+".drop")
		q.dropRngPrefix = q.prefix
	}
	if q.dropRng != nil && q.prefix != q.dropRngPrefix {
		panic("temporal/matcher: late SubstreamPrefix mutation after RNG creation for queue " + q.name)
	}
	if q.rng != nil && q.prefix != q.rngPrefix {
		panic("temporal/matcher: late SubstreamPrefix mutation after RNG creation for queue " + q.name)
	}
}

// SetDispatchDelay sets the dispatch delay.
func (q *MatchQueue) SetDispatchDelay(ms int64) {
	if ms < 0 {
		ms = 0
	}
	q.dispatchDelay = ms
}

func (q *MatchQueue) setRate(rate, burst float64) {
	q.bucket = NewTokenBucket(rate, burst)
}

// SetRate reconfigures the dispatch token bucket (exported for binder).
func (q *MatchQueue) SetRate(rate, burst float64) { q.setRate(rate, burst) }

// SpoolDropRate returns the spool drop rate (for tests).
func (q *MatchQueue) SpoolDropRate() float64 { return q.spoolDropRate }

// DispatchDelay returns the dispatch delay (for tests).
func (q *MatchQueue) DispatchDelay() int64 { return q.dispatchDelay }

// SpoolOnly spools a task without sync-match (rescheduled path). It never
// rejects: matching is persistence-backed (unbounded), so the capacity
// field is not enforced and the call always returns true. Zero RNG draws.
func (q *MatchQueue) SpoolOnly(t Task) bool {
	q.backlog = append(q.backlog, t)
	q.spooled++
	return true
}

// ShouldDropOnAdd checks the spool drop RNG and increments dropped counter.
func (q *MatchQueue) ShouldDropOnAdd() bool {
	if q.spoolDropRate <= 0 {
		return false
	}
	if q.dropRng.Float64() < q.spoolDropRate {
		q.droppedDrop++
		return true
	}
	return false
}

// Stats returns run counters. rejected is always zero (no capacity reject —
// retained in the signature for caller compatibility).
func (q *MatchQueue) Stats() (synced, spooled, rejected, dispatched, refused, dropped uint64) {
	return q.synced, q.spooled, q.rejected, q.dispatched, q.bucket.Refused(), q.droppedDrop
}

// HasRNG reports whether the throttle substream was created (rate>0).
func (q *MatchQueue) HasRNG() bool { return q.rng != nil }

func src(sim *core.Simulation, prefix, name string) core.RNG {
	// Double "temporal." is intentional — binder path isolation post-hybrid (ADR-091).
	if prefix != "" {
		return sim.Substream("model.temporal.temporal." + prefix + "." + name)
	}
	return sim.Substream("model.temporal.temporal." + name)
}

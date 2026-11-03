// Package faults is the fault catalog that ships with faultsched: the
// project-free half of fault injection.
//
// A fault is split in two by design:
//
//   - WHAT fires is generic and lives here — the fault schedule kind, the
//     half-open window query, the rate semantics and the effect the fault asks
//     for. faultsched.State(seed, elapsedMs) is a pure function, so every seam
//     that queries it sees the same answer whatever its query rate.
//   - WHERE it lands is project-specific and lives with the project — Tasqueue
//     wraps broker.Enqueue, Temporal wraps its matching path, Raft wraps its
//     transport. Only the project knows where work is admitted, attempted or
//     persisted, so a project file maps this package's Roles onto its own seams.
//
// The contract between the two halves is a project's honored set: every kind it
// can inject, with the Role that says where. Check refuses a schedule naming
// anything else (ADR-100: a kind a leg cannot honor must fail loudly, never
// silently no-op).
package faults

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lawRathod/queuesim/godes/faultsched"
)

// Role names the kind of seam a fault acts on, independent of any project. The
// set is derived from the fault kinds faultsched already defines — every kind
// below has exactly one home in the Roles table, so no kind is left without a
// role to act on.
type Role string

const (
	// RoleAdmit is work entering the system: a task admitted to a queue.
	// Tasqueue's seam is broker.Enqueue; Temporal's is its matching AddTask
	// path; Raft's is its transport send.
	RoleAdmit Role = "admit"
	// RoleDeliver is work handed to a worker: a poll served, a message
	// received.
	RoleDeliver Role = "deliver"
	// RoleAttempt is one attempt's execution: the handler, the activity, the
	// apply.
	RoleAttempt Role = "attempt"
	// RoleLatency is added delay on a path, bounded by the run's deadlines.
	RoleLatency Role = "latency"
	// RoleCapacity is how much may run at once.
	RoleCapacity Role = "capacity"
	// RolePersist is storage: write latency and write failure.
	RolePersist Role = "persist"
	// RoleArrival is the arrival schedule itself, which the harness owns.
	RoleArrival Role = "arrival"
)

// Roles maps every faultsched kind to the Role that applies it. It is the
// derivation artifact: the four-role set a first guess would produce strands
// poller_min (capacity, not latency) and backlog_surge (arrival, not admit), so
// the roles are read off the kinds rather than invented.
var Roles = map[faultsched.Kind]Role{
	faultsched.KindAddDrop:          RoleAdmit,    // tasks dropped entering the queue
	faultsched.KindWfDrop:           RoleAdmit,    // wf-class tasks dropped at the matching queue
	faultsched.KindBacklogSurge:     RoleArrival,  // a secondary arrival burst at StartMs
	faultsched.KindPollDrop:         RoleDeliver,  // a poll fails, no dispatch
	faultsched.KindPollJitter:       RoleDeliver,  // a long-poll expires early
	faultsched.KindDuplicate:        RoleDeliver,  // reserved: delivered twice
	faultsched.KindActivityFailure:  RoleAttempt,  // attempts fail
	faultsched.KindTimerLoad:        RoleAttempt,  // work sleeps before it runs
	faultsched.KindS2CShorten:       RoleAttempt,  // the attempt's deadline shrinks
	faultsched.KindS2STimeout:       RoleLatency,  // schedule-to-start deadline
	faultsched.KindTransferDelay:    RoleLatency,  // one delay per queue crossing
	faultsched.KindPartition:        RoleLatency,  // reserved: a path is cut
	faultsched.KindPollerMin:        RoleCapacity, // forced poller count
	faultsched.KindPersistenceStall: RolePersist,  // write latency multiplier
	faultsched.KindPersistenceFail:  RolePersist,  // reserved: writes fail
}

// Effects is what the active faults of one Role ask a seam to do. A seam reads
// the field it can honor and ignores the rest.
type Effects struct {
	// Drop swallows the event (an admitted job, a delivery).
	Drop bool
	// Fail fails the attempt.
	Fail bool
	// Delay is added latency before the event completes.
	Delay time.Duration
}

// Fault is one injectable fault: the schedule kind it honors, the effect it
// asks for, and how an active window's rate turns into that effect.
type Fault struct {
	// Kind is the faultsched kind whose window activates this fault.
	Kind faultsched.Kind
	// Effect describes, in one line, what an active window does.
	Effect string
	// Eval turns an active rate and a coin draw in [0,1) into effects. The
	// coin is drawn by the seam, not here, so per-event draws stay leg-local
	// (faultsched's contract: the schedule is shared, coin outcomes are not).
	Eval func(rate, coin float64) Effects
}

// Catalog is every fault implemented, in deterministic order — the order
// Active draws coins in.
var Catalog = []Fault{Drop, Failure, Slow}

// Active returns the combined effect of every catalog fault of role r that is
// active at elapsedMs, drawing one coin per active fault in catalog order. A nil
// source means no schedule, which means no faults.
func Active(sched faultsched.Source, seed, elapsedMs int64, r Role, coin func() float64) Effects {
	if sched == nil {
		return Effects{}
	}
	var out Effects
	for _, af := range sched.State(seed, elapsedMs) {
		f, ok := lookup(af.Kind)
		if !ok || Roles[af.Kind] != r {
			continue
		}
		draw := 1.0
		if coin != nil {
			draw = coin()
		}
		eff := f.Eval(af.Rate, draw)
		out.Drop = out.Drop || eff.Drop
		out.Fail = out.Fail || eff.Fail
		if eff.Delay > out.Delay {
			out.Delay = eff.Delay
		}
	}
	return out
}

// Check returns the kinds in sched that honored does not cover, sorted. An
// empty result means every window names a fault this project can inject.
func Check(sched *faultsched.Schedule, honored map[faultsched.Kind]Role) []faultsched.Kind {
	var missing []faultsched.Kind
	for _, w := range sched.Windows() {
		if _, ok := honored[w.Kind]; !ok {
			missing = append(missing, w.Kind)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

// UnhonoredError names the unhonorable kinds and the role each belongs to, so a
// schedule is refused with the reason and not just the kind string.
func UnhonoredError(missing []faultsched.Kind) error {
	if len(missing) == 0 {
		return nil
	}
	parts := make([]string, 0, len(missing))
	for _, k := range missing {
		parts = append(parts, fmt.Sprintf("%s (%s)", k, Roles[k]))
	}
	return fmt.Errorf("fault kind(s) this project cannot inject: %s", strings.Join(parts, ", "))
}

// lookup finds the catalog fault honoring kind.
func lookup(kind faultsched.Kind) (Fault, bool) {
	for _, f := range Catalog {
		if f.Kind == kind {
			return f, true
		}
	}
	return Fault{}, false
}

package faults

import "github.com/lawRathod/queuesim/godes/faultsched"

// Drop swallows work as it is admitted: while an add_drop window is active, a
// fraction of admitted jobs never enters the queue.
//
// The job is accepted by the server (its status row is written before the
// broker call), so a dropped job is admitted-and-lost, not rejected — which is
// why a seam applying this must count what it swallowed for the run's
// quiescence accounting.
//
// Role: RoleAdmit. faultsched's own description of add_drop — "drops a fraction
// of tasks entering the matching queue" — is exactly this.
var Drop = Fault{
	Kind:   faultsched.KindAddDrop,
	Effect: "swallow a fraction of admitted jobs",
	Eval: func(rate, coin float64) Effects {
		return Effects{Drop: coin < rate}
	},
}

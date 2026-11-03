package faults

import "github.com/lawRathod/queuesim/godes/faultsched"

// Failure fails an attempt: while an activity_failure window is active, every
// attempt it covers fails, and the retry budget decides whether the job still
// completes. Whether a failure is terminal is the workload's business, not this
// fault's.
//
// Rate is a scalar arm flag, not a probability — faultsched documents
// activity_failure as "Rate = scalar (probability per attempt is leg-local)",
// so the per-attempt draw stays with the leg (the coin this Eval receives).
// A rate of 0 is inactive in effect even though its window is open.
//
// Role: RoleAttempt.
var Failure = Fault{
	Kind:   faultsched.KindActivityFailure,
	Effect: "fail the attempt",
	Eval: func(rate, coin float64) Effects {
		return Effects{Fail: rate > 0 && coin < rate}
	},
}

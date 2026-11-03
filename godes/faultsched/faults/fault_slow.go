package faults

import (
	"time"

	"github.com/lawRathod/queuesim/godes/faultsched"
)

// Slow makes an attempt take longer: while a timer_load window is active, the
// work sleeps Rate milliseconds before it runs.
//
// Rate is a duration in milliseconds, not a probability — faultsched documents
// timer_load as "each workflow sleeps Rate ms before scheduling its activity".
// Combined with a per-attempt deadline this is how a timeout is injected: the
// sleep outlasts the deadline.
//
// Role: RoleAttempt.
var Slow = Fault{
	Kind:   faultsched.KindTimerLoad,
	Effect: "delay the attempt by Rate ms",
	Eval: func(rate, _ float64) Effects {
		if rate <= 0 {
			return Effects{}
		}
		return Effects{Delay: time.Duration(rate) * time.Millisecond}
	},
}

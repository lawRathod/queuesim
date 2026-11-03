package core_test

import (
	"fmt"

	"github.com/lawRathod/queuesim/godes/core"
)

func ExampleSimulation_ordering() {
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		panic(err)
	}
	trace := make([]string, 0, 3)

	sim.ScheduleAt(core.NewSimTime(5), func(*core.Simulation) { trace = append(trace, "event_A") })
	sim.ScheduleAt(core.NewSimTime(2), func(*core.Simulation) { trace = append(trace, "event_B") })
	sim.ScheduleAt(core.NewSimTime(8), func(*core.Simulation) { trace = append(trace, "event_C") })

	sim.Run()
	fmt.Println(trace)
	// Output: [event_B event_A event_C]
}

package resource

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// Container models a fluid tank (SimPy's Container): capacity, current
// level, and put/take operations that may wait. Put blocks while there is
// not enough free space for the requested amount; Take blocks while there
// is not enough fluid. Waiters are served FIFO by registration, and each
// waiter carries its own amount — a waiter is only granted once its
// specific amount fits/available, so a taker needing 10 will not be
// granted when only 2 is present.
type Container struct {
	sim      *core.Simulation
	capacity float64
	level    float64
	putters  []*amountWaiter
	takers   []*amountWaiter
	puts     uint64
	takes    uint64
}

// amountWaiter is a queued operator carrying the amount it needs.
type amountWaiter struct {
	amount float64
	fn     func(*core.Simulation)
}

// NewContainer creates a Container with the given capacity and initial
// level. Panics if capacity <= 0, level < 0, level > capacity, or sim is
// nil.
func NewContainer(sim *core.Simulation, capacity, level float64) *Container {
	if sim == nil {
		panic("resource: NewContainer requires a non-nil simulation")
	}
	if capacity <= 0 {
		panic("resource: capacity must be > 0")
	}
	if level < 0 || level > capacity {
		panic("resource: initial level out of range")
	}
	return &Container{
		sim:      sim,
		capacity: capacity,
		level:    level,
		putters:  []*amountWaiter{},
		takers:   []*amountWaiter{},
	}
}

// Level returns the current fluid level.
func (c *Container) Level() float64 { return c.level }

// Capacity returns the container capacity.
func (c *Container) Capacity() float64 { return c.capacity }

// Free returns the space remaining.
func (c *Container) Free() float64 { return c.capacity - c.level }

// Puts returns the total number of put operations.
func (c *Container) Puts() uint64 { return c.puts }

// Takes returns the total number of take operations.
func (c *Container) Takes() uint64 { return c.takes }

// TryPut adds amount without waiting. Returns true if the amount fit
// (level+amount <= capacity), false otherwise (no change).
func (c *Container) TryPut(amount float64) bool {
	if amount < 0 {
		return false
	}
	if c.level+amount > c.capacity {
		return false
	}
	c.level += amount
	c.puts++
	return true
}

// TryTake removes amount without waiting. Returns true if available
// (level >= amount), false otherwise (no change).
func (c *Container) TryTake(amount float64) bool {
	if amount < 0 {
		return false
	}
	if c.level < amount {
		return false
	}
	c.level -= amount
	c.takes++
	return true
}

// Put adds amount, invoking fn immediately if it fits or queueing it (FIFO)
// until space frees. fn runs at the current sim time on grant.
func (c *Container) Put(amount float64, fn func(*core.Simulation)) {
	if amount < 0 {
		return
	}
	if c.level+amount <= c.capacity {
		c.level += amount
		c.puts++
		runWaiter(fn, c.sim)
		c.serve() // a filled tank may satisfy waiting takers
		return
	}
	c.putters = append(c.putters, &amountWaiter{amount: amount, fn: fn})
}

// Take removes amount, invoking fn immediately if available or queueing
// it (FIFO) until the level suffices.
func (c *Container) Take(amount float64, fn func(*core.Simulation)) {
	if amount < 0 {
		return
	}
	if c.level >= amount {
		c.level -= amount
		c.takes++
		runWaiter(fn, c.sim)
		c.serve() // freeing space may satisfy waiting putters
		return
	}
	c.takers = append(c.takers, &amountWaiter{amount: amount, fn: fn})
}

// serve grants waiting putters and takers to a fixed point. Each iteration
// either grants a waiter (which shrinks the waiter lists — guaranteed
// progress) or, on no progress, breaks. Grants are served FIFO within each
// side, and the two sides alternate so a filled putter can wake a taker and
// a drained taker can wake a putter in the same sim time.
func (c *Container) serve() {
	for {
		progressed := false
		for len(c.takers) > 0 && c.level >= c.takers[0].amount {
			w := c.takers[0]
			c.takers = c.takers[1:]
			c.level -= w.amount
			c.takes++
			runWaiter(w.fn, c.sim)
			progressed = true
		}
		for len(c.putters) > 0 && c.level+c.putters[0].amount <= c.capacity {
			w := c.putters[0]
			c.putters = c.putters[1:]
			c.level += w.amount
			c.puts++
			runWaiter(w.fn, c.sim)
			progressed = true
		}
		if !progressed {
			break
		}
	}
}

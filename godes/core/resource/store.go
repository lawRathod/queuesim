package resource

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// Store is a bounded generic buffer (SimPy's Store): Put places an item
// (blocking while full), Get retrieves one (blocking while empty).
// Retrieval order is FIFO by default. Waiters are served FIFO.
//
// A queueing Put carries its item in the waiter (storeWaiter), so when a
// slot frees the item is placed by the engine, not by the caller's
// continuation. A queueing Get carries the destination pointer/closure
// that receives the popped item.
type Store struct {
	sim      *core.Simulation
	capacity int
	items    []any
	putters  []*storeWaiter
	getters  []*storeWaiter
	puts     uint64
	gets     uint64
}

// storeWaiter is a queued put (carrying its item) or get (carrying its
// receiver). put is non-nil for putters, get for getters.
type storeWaiter struct {
	put any
	get func(any)
	fn  func(*core.Simulation)
}

// NewStore creates a Store with the given capacity. Panics if capacity <=
// 0 or sim is nil.
func NewStore(sim *core.Simulation, capacity int) *Store {
	if sim == nil {
		panic("resource: NewStore requires a non-nil simulation")
	}
	if capacity <= 0 {
		panic("resource: capacity must be > 0")
	}
	return &Store{
		sim:      sim,
		capacity: capacity,
		items:    []any{},
		putters:  []*storeWaiter{},
		getters:  []*storeWaiter{},
	}
}

// Len returns the current number of items.
func (s *Store) Len() int { return len(s.items) }

// Capacity returns the configured capacity.
func (s *Store) Capacity() int { return s.capacity }

// Free returns the remaining item slots.
func (s *Store) Free() int { return s.capacity - len(s.items) }

// Puts returns the total number of put operations.
func (s *Store) Puts() uint64 { return s.puts }

// Gets returns the total number of get operations.
func (s *Store) Gets() uint64 { return s.gets }

// TryPut places item without waiting. Returns true if a slot was free,
// false otherwise (no change).
func (s *Store) TryPut(item any) bool {
	if len(s.items) >= s.capacity {
		return false
	}
	s.items = append(s.items, item)
	s.puts++
	return true
}

// TryGet retrieves the oldest item without waiting. Returns (item, true)
// if available, (nil, false) otherwise.
func (s *Store) TryGet() (any, bool) {
	if len(s.items) == 0 {
		return nil, false
	}
	it := s.items[0]
	s.items = s.items[1:]
	s.gets++
	return it, true
}

// Put places item, invoking fn immediately if a slot is free or queueing
// it (FIFO) until a slot frees. The item is placed at grant.
func (s *Store) Put(item any, fn func(*core.Simulation)) {
	if len(s.items) < s.capacity {
		s.items = append(s.items, item)
		s.puts++
		runWaiter(fn, s.sim)
		s.serve()
		return
	}
	s.putters = append(s.putters, &storeWaiter{put: item, get: nil, fn: fn})
}

// Get retrieves the oldest item, invoking recv(item) immediately if
// available or queueing it (FIFO) until an item is placed. recv runs at
// grant with the popped item.
func (s *Store) Get(recv func(any)) {
	if len(s.items) > 0 {
		it := s.items[0]
		s.items = s.items[1:]
		s.gets++
		recv(it)
		s.serve()
		return
	}
	s.getters = append(s.getters, &storeWaiter{put: nil, get: recv})
}

// serve grants waiting getters and putters to a fixed point (FIFO within
// each side; getters first so a slot is consumed before a putter refills).
func (s *Store) serve() {
	for {
		progressed := false
		for len(s.getters) > 0 && len(s.items) > 0 {
			w := s.getters[0]
			s.getters = s.getters[1:]
			it := s.items[0]
			s.items = s.items[1:]
			s.gets++
			if w.get != nil {
				w.get(it)
			}
			progressed = true
		}
		for len(s.putters) > 0 && len(s.items) < s.capacity {
			w := s.putters[0]
			s.putters = s.putters[1:]
			s.items = append(s.items, w.put)
			s.puts++
			runWaiter(w.fn, s.sim)
			progressed = true
		}
		if !progressed {
			break
		}
	}
}

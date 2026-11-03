package faultsched

import (
	"reflect"
	"sync"
	"testing"
)

// TestCoverageQueriesIndependentOfQuanta pins the ADR-112 semantics that
// query count and quantum count are different things: many queries inside
// one quantum increment Queries without growing QueriedQuant.
func TestCoverageQueriesIndependentOfQuanta(t *testing.T) {
	c := NewCoverage(100, 1000)
	for i := 0; i < 7; i++ {
		c.Add(250, nil) // all inside quantum 2
	}
	if got := c.Queries(); got != 7 {
		t.Fatalf("Queries() = %d, want 7", got)
	}
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{2}) {
		t.Fatalf("QueriedQuant() = %v, want [2]", got)
	}
}

// TestCoverageRepeatQueriesIdempotent pins that re-querying a quantum, with
// or without active faults, never duplicates a bitmap entry.
func TestCoverageRepeatQueriesIdempotent(t *testing.T) {
	c := NewCoverage(100, 1000)
	add := []ActiveFault{{Kind: KindActivityFailure, Rate: 1}}
	for i := 0; i < 5; i++ {
		c.Add(300, add)
		c.Add(300, nil)
	}
	if got := c.ActiveQuant(KindActivityFailure); !reflect.DeepEqual(got, []int64{3}) {
		t.Fatalf("ActiveQuant = %v, want [3]", got)
	}
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{3}) {
		t.Fatalf("QueriedQuant = %v, want [3]", got)
	}
	if got := c.Queries(); got != 10 {
		t.Fatalf("Queries() = %d, want 10", got)
	}
}

// TestCoverageNilActiveStillRecords pins the non-vacuous record: a query
// with no active faults still marks the quantum and counts.
func TestCoverageNilActiveStillRecords(t *testing.T) {
	c := NewCoverage(100, 1000)
	c.Add(0, nil)
	c.Add(999, nil)
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{0, 9}) {
		t.Fatalf("QueriedQuant = %v, want [0 9]", got)
	}
	if got := c.Queries(); got != 2 {
		t.Fatalf("Queries() = %d, want 2", got)
	}
	if got := c.ActiveQuant(KindAddDrop); got != nil {
		t.Fatalf("ActiveQuant(add_drop) = %v, want nil", got)
	}
}

// TestCoveragePerKindSeparation pins that per-kind bitmaps stay separate
// and only the queried-and-active combinations are marked.
func TestCoveragePerKindSeparation(t *testing.T) {
	c := NewCoverage(100, 1000)
	c.Add(100, []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}, {Kind: KindWfDrop, Rate: 0.5}})
	c.Add(200, []ActiveFault{{Kind: KindWfDrop, Rate: 0.5}})
	c.Add(500, []ActiveFault{{Kind: KindActivityFailure, Rate: 1}})
	cases := []struct {
		kind Kind
		want []int64
	}{
		{KindAddDrop, []int64{1}},
		{KindWfDrop, []int64{1, 2}},
		{KindActivityFailure, []int64{5}},
		{KindPollDrop, nil},
	}
	for _, tc := range cases {
		if got := c.ActiveQuant(tc.kind); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ActiveQuant(%s) = %v, want %v", tc.kind, got, tc.want)
		}
	}
}

// TestCoverageHorizonClamping pins the horizon edges: a query past the
// horizon is counted but records no quantum; a negative time floors to a
// negative index and records nothing; a zero horizon has no quanta.
func TestCoverageHorizonClamping(t *testing.T) {
	c := NewCoverage(100, 1000)                                // quanta 0..9
	c.Add(1000, []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}}) // quantum 10, past horizon
	c.Add(-1, []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}})   // negative
	c.Add(999, nil)                                            // quantum 9, in horizon
	if got := c.Queries(); got != 3 {
		t.Fatalf("Queries() = %d, want 3 (all queries counted)", got)
	}
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{9}) {
		t.Fatalf("QueriedQuant = %v, want [9]", got)
	}
	if got := c.ActiveQuant(KindAddDrop); got != nil {
		t.Fatalf("ActiveQuant(add_drop) = %v, want nil (both adds out of horizon)", got)
	}

	zero := NewCoverage(100, 0)
	zero.Add(0, []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}})
	if got := zero.Queries(); got != 1 {
		t.Fatalf("zero-horizon Queries() = %d, want 1", got)
	}
	if got := zero.QueriedQuant(); got != nil {
		t.Fatalf("zero-horizon QueriedQuant() = %v, want nil", got)
	}
	if got := zero.ActiveFrac(KindAddDrop); got != 0 {
		t.Fatalf("zero-horizon ActiveFrac() = %v, want 0", got)
	}
}

// TestCoverageQuantumIndex pins the floor semantics, including the negative
// case (Go's truncating division would round -1/100 to 0, not -1).
func TestCoverageQuantumIndex(t *testing.T) {
	c := NewCoverage(100, 60000)
	cases := []struct {
		ms   int64
		want int64
	}{
		{0, 0}, {1, 0}, {99, 0}, {100, 1}, {199, 1}, {60000, 600}, {-1, -1}, {-100, -1}, {-101, -2},
	}
	for _, tc := range cases {
		if got := c.QuantumIndex(tc.ms); got != tc.want {
			t.Errorf("QuantumIndex(%d) = %d, want %d", tc.ms, got, tc.want)
		}
	}
	var nilCov *Coverage
	if got := nilCov.QuantumIndex(500); got != 500 {
		t.Errorf("nil QuantumIndex(500) = %d, want 500", got)
	}
}

// TestCoverageNilReceiverIsOff pins the byte-neutral off state: a nil
// *Coverage records nothing and reports empty.
func TestCoverageNilReceiverIsOff(t *testing.T) {
	var c *Coverage
	c.Add(0, []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}})
	if got := c.Queries(); got != 0 {
		t.Fatalf("nil Queries() = %d, want 0", got)
	}
	if got := c.QueriedQuant(); got != nil {
		t.Fatalf("nil QueriedQuant() = %v, want nil", got)
	}
	if got := c.ActiveQuant(KindAddDrop); got != nil {
		t.Fatalf("nil ActiveQuant() = %v, want nil", got)
	}
}

// TestCoverageActiveFrac pins the horizon-denominator fraction.
func TestCoverageActiveFrac(t *testing.T) {
	c := NewCoverage(100, 1000) // 10 quanta
	c.Add(0, []ActiveFault{{Kind: KindWfDrop, Rate: 0.3}})
	c.Add(100, []ActiveFault{{Kind: KindWfDrop, Rate: 0.3}})
	if got := c.ActiveFrac(KindWfDrop); got != 0.2 {
		t.Fatalf("ActiveFrac(wf_drop) = %v, want 0.2", got)
	}
	if got := c.ActiveFrac(KindAddDrop); got != 0 {
		t.Fatalf("ActiveFrac(add_drop) = %v, want 0", got)
	}
}

// TestCoverageUnevenHorizonCeil pins the ceil bitmap sizing: a horizon that
// is not a whole number of quanta still allocates the final partial
// quantum.
func TestCoverageUnevenHorizonCeil(t *testing.T) {
	c := NewCoverage(100, 250) // ceil(2.5) = 3 quanta (0..2)
	c.Add(200, nil)
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{2}) {
		t.Fatalf("QueriedQuant = %v, want [2]", got)
	}
	c.Add(250, nil) // quantum 2 (partial quantum), in range
	if got := c.QueriedQuant(); !reflect.DeepEqual(got, []int64{2}) {
		t.Fatalf("after 250ms QueriedQuant = %v, want [2]", got)
	}
	c.Add(300, nil) // quantum 3, past horizon
	if got := c.Queries(); got != 3 {
		t.Fatalf("Queries() = %d, want 3", got)
	}
}

// TestCoverageConcurrentAdd pins the recorder's thread-safety contract: the
// real-server leg queries the schedule from activity-execution and
// gRPC-handler goroutines, so concurrent Add/accessor calls must not race,
// panic, or lose a query. Run under -race for the memory-ordering check.
func TestCoverageConcurrentAdd(t *testing.T) {
	c := NewCoverage(100, 10000)
	const goroutines, perG = 16, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				q := int64((g*perG + i) % 100)
				c.Add(q*100, []ActiveFault{{Kind: KindActivityFailure, Rate: 1}})
				_ = c.Queries()
				_ = c.ActiveQuant(KindActivityFailure)
				_ = c.QueriedQuant()
			}
		}(g)
	}
	wg.Wait()
	if got := c.Queries(); got != goroutines*perG {
		t.Fatalf("concurrent Queries() = %d, want %d (lost update)", got, goroutines*perG)
	}
	if got := len(c.QueriedQuant()); got != 100 {
		t.Fatalf("concurrent QueriedQuant() = %d quanta, want 100", got)
	}
	if got := len(c.ActiveQuant(KindActivityFailure)); got != 100 {
		t.Fatalf("concurrent ActiveQuant() = %d quanta, want 100", got)
	}
}

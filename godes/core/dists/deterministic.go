package dists

// DeterministicDistribution always returns the same value. Construct via
// NewDeterministic. Consumes zero source draws — fully deterministic by
// construction, useful as the "zero-variance" distribution.
type DeterministicDistribution struct {
	value float64
}

// NewDeterministic creates a deterministic distribution that always
// returns value.
func NewDeterministic(value float64) *DeterministicDistribution {
	return &DeterministicDistribution{value: value}
}

// Sample returns the fixed value.
func (d *DeterministicDistribution) Sample() float64 { return d.value }

// Value returns the fixed value.
func (d *DeterministicDistribution) Value() float64 { return d.value }

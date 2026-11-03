package dists

// EmpiricalDistribution samples from a piecewise-constant empirical
// histogram defined by value/weight pairs. The zero value is not usable;
// construct via NewEmpirical. Each Sample consumes one draw.
type EmpiricalDistribution struct {
	values  []float64
	weights []float64
	cdf     []float64
	src     Source
}

// NewEmpirical creates an empirical distribution over the given values
// with the given weights. Panics if len(values) == 0, len(weights) !=
// len(values), any weight < 0, total weight == 0, or src is nil.
func NewEmpirical(values, weights []float64, src Source) *EmpiricalDistribution {
	if len(values) == 0 {
		panic("dists: Empirical requires at least one value")
	}
	if len(weights) != len(values) {
		panic("dists: Empirical requires weights and values of equal length")
	}
	if src == nil {
		panic("dists: Empirical requires a non-nil Source")
	}
	total := 0.0
	for _, w := range weights {
		if w < 0 {
			panic("dists: Empirical weights must be non-negative")
		}
		total += w
	}
	if total == 0 {
		panic("dists: Empirical requires positive total weight")
	}
	cdf := make([]float64, len(values))
	acc := 0.0
	for i, w := range weights {
		acc += w / total
		cdf[i] = acc
	}
	// Normalize the last bin to exactly 1.0 to make the search robust.
	cdf[len(cdf)-1] = 1.0
	return &EmpiricalDistribution{values: values, weights: weights, cdf: cdf, src: src}
}

// Sample draws one value from the empirical distribution via inverse CDF
// search.
func (d *EmpiricalDistribution) Sample() float64 {
	u := d.src.Float64()
	for i, c := range d.cdf {
		if u < c {
			return d.values[i]
		}
	}
	return d.values[len(d.values)-1]
}

// Values returns a copy of the value list.
func (d *EmpiricalDistribution) Values() []float64 {
	out := make([]float64, len(d.values))
	copy(out, d.values)
	return out
}

// Weights returns a copy of the weight list.
func (d *EmpiricalDistribution) Weights() []float64 {
	out := make([]float64, len(d.weights))
	copy(out, d.weights)
	return out
}

// Mean returns the weighted mean of the support.
func (d *EmpiricalDistribution) Mean() float64 {
	total := 0.0
	for _, w := range d.weights {
		total += w
	}
	m := 0.0
	for i := range d.values {
		m += d.values[i] * d.weights[i] / total
	}
	return m
}

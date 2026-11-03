package dists

// UniformDistribution samples a continuous uniform distribution over
// [lo, hi). The zero value is not usable; construct via NewUniform.
// Each Sample consumes exactly one source draw.
type UniformDistribution struct {
	lo  float64
	hi  float64
	src Source
}

// NewUniform creates a UniformDistribution over [lo, hi) sampling from src.
// Panics if hi <= lo or src is nil.
func NewUniform(lo, hi float64, src Source) *UniformDistribution {
	if hi <= lo {
		panic("dists: Uniform requires hi > lo")
	}
	if src == nil {
		panic("dists: Uniform requires a non-nil Source")
	}
	return &UniformDistribution{lo: lo, hi: hi, src: src}
}

// Sample draws one value in [lo, hi).
func (d *UniformDistribution) Sample() float64 {
	return d.lo + (d.hi-d.lo)*d.src.Float64()
}

// Lo returns the lower bound.
func (d *UniformDistribution) Lo() float64 { return d.lo }

// Hi returns the upper bound.
func (d *UniformDistribution) Hi() float64 { return d.hi }

// IntegerDistribution samples a discrete uniform integer in {min, …, max}
// (inclusive). Construct via NewInteger. Each Sample consumes one draw.
type IntegerDistribution struct {
	min  int64
	max  int64
	span float64
	src  Source
}

// NewInteger creates a discrete uniform distribution over the inclusive
// range [min, max]. Panics if max < min or src is nil.
func NewInteger(min, max int64, src Source) *IntegerDistribution {
	if max < min {
		panic("dists: Integer requires max >= min")
	}
	if src == nil {
		panic("dists: Integer requires a non-nil Source")
	}
	return &IntegerDistribution{min: min, max: max, span: float64(max - min + 1), src: src}
}

// Sample draws one integer in {min, …, max}.
func (d *IntegerDistribution) Sample() int64 {
	return d.min + int64(d.src.Float64()*d.span)
}

// Mean returns the expected value (min+max)/2.
func (d *IntegerDistribution) Mean() float64 { return (float64(d.min) + float64(d.max)) / 2 }

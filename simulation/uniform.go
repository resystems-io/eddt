package simulation

import (
	"math"
	"math/rand/v2"
	"time"

	"gonum.org/v1/gonum/stat/distuv"
)

type Rate interface {
	// Number of operations per second chosen from the distribution.
	Ops() float64
}

// FixedRate defines a fixed number of operations per second.
type FixedRate struct {
	OpsPerSecond float64
}

func (r *FixedRate) Ops() float64 {
	return r.OpsPerSecond
}

type TimeDistribution interface {
	// Duration chosen from the distribution.
	Duration() time.Duration
}

type ProbabilityDistribution interface {
	// Probability chosen from the distribution over [0,1].
	Probability() float64
}

type IntDistribution interface {
	// Number chosen from the distribution.
	Int() int
}

// Range defines the [min, max)
type IntRange struct {
	Min int
	Max int
}

// Range defines the [0,1)
type UnitRange struct {
}

func (r *UnitRange) Min() int {
	return 0
}

func (r *UnitRange) Max() int {
	return 1
}

// Rate defines the [min, max) rate of operations per second.
type UniformRate struct {
	IntRange
}

func (r *UniformRate) Ops() int {
	return rand.IntN(r.Max-r.Min) + r.Min
}

// Range defines the [min, max] value.
type UniformIntDistribution struct {
	IntRange
}

func (r *UniformIntDistribution) Int() int {
	return rand.IntN(r.Max-r.Min) + r.Min
}

// Normal distribution of integers with bounds
//
// Integers are generated based on µ and σ, but clipped to the range [min,max].
type BoundedNormalIntDistribution struct {
	distuv.Normal
	IntRange
}

func (r *BoundedNormalIntDistribution) Int() int {
	x := int(math.Round(r.Rand()))
	x = max(r.Min, x)
	x = min(r.Max, x)
	return x
}

// Range defines the [min, max) time range.
type UniformTimeDistribution struct {
	Min time.Duration
	Max time.Duration
}

func (r *UniformTimeDistribution) Duration() time.Duration {

	// TODO consider using gonum/stat/distuv/Uniform

	// Calculate the size of the range
	rangeDuration := r.Max - r.Min
	if rangeDuration <= 0 {
		return r.Min // or handle the error
	}

	// Get a random int64 within the range [0, rangeDuration)
	randomOffset := rand.Int64N(int64(rangeDuration))

	// Add the minimum duration to the random offset
	return r.Min + time.Duration(randomOffset)
}

// Range defines subset of [0, 1).
type UniformProbabilityDistribution struct {
	Min float64
	Max float64
}

func (r *UniformProbabilityDistribution) Probability() float64 {
	return rand.NormFloat64()*(r.Max-r.Min) + r.Min
}

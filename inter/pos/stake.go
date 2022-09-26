package pos

import (
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

type (
	// Weight amount.
	Weight uint32
)

type (
	// WeightCounterProvider providers weight counter.
	WeightCounterProvider func() *WeightCounter

	// WeightCounter counts weights.
	WeightCounter struct {
		validators Validators
		already    []bool // idx.Validator -> bool

		Quorum Weight
		sum    Weight
	}
)

// NewCounter constructor.
func (vv Validators) NewCounter() *WeightCounter {
	return newWeightCounter(vv)
}

func newWeightCounter(vv Validators) *WeightCounter {
	return &WeightCounter{
		validators: vv,
		Quorum:     vv.Quorum(),
		already:    make([]bool, vv.Len()),
		sum:        0,
	}
}

// Count validator and return true if it hadn't counted before.
func (s *WeightCounter) Count(v idx.ValidatorID) bool {
	validatorIdx := s.validators.GetIdx(v)
	return s.CountByIdx(validatorIdx)
}

// CountByIdx validator and return true if it hadn't counted before.
func (s *WeightCounter) CountByIdx(validatorIdx idx.Validator) bool {
	if s.already[validatorIdx] {
		return false
	}
	s.already[validatorIdx] = true

	s.sum += s.validators.GetWeightByIdx(validatorIdx)
	return true
}

// HasQuorum achieved.
func (s *WeightCounter) HasQuorum() bool {
	return s.sum >= s.Quorum
}

// Sum of counted weights.
func (s *WeightCounter) Sum() Weight {
	return s.sum
}

// Is validator already counted
func (s *WeightCounter) Already(validatorIdx idx.Validator) bool {
	return s.already[validatorIdx]
}

// Number of validators counted
func (s *WeightCounter) NumCounted() int {
	num := 0
	for _, counted := range s.already {
		if counted {
			num++
		}
	}
	return num
}

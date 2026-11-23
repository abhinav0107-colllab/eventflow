package jobs

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays using capped exponential backoff with
// "equal jitter": half of the exponential delay is fixed and the other half
// is random. This keeps a guaranteed minimum spacing between attempts while
// de-synchronising retries of jobs that failed together.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	// Rand returns a float in [0,1). Overridable for deterministic tests.
	Rand func() float64
}

// Delay returns the delay before attempt number `attempt`+1, given that
// `attempt` (>=1) attempts have already failed.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base, max := b.Base, b.Max
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) || math.IsInf(exp, 0) {
		exp = float64(max)
	}
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	half := exp / 2
	return time.Duration(half + r()*half)
}

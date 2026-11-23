package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// CPUPayload is the CPU_INTENSIVE_COMPUTATION input.
type CPUPayload struct {
	Operation string `json:"operation"` // "prime_count" or "hash_chain"
	N         int    `json:"n"`
	Seed      string `json:"seed"`
}

// Limits keep a single job bounded in memory and time.
const (
	maxPrimeN = 20_000_000
	maxHashN  = 20_000_000
	checkMask = 1<<14 - 1 // check for cancellation every 16k iterations
)

// CPU performs real CPU-bound work and honours cancellation while doing so.
type CPU struct{}

func (CPU) parse(raw json.RawMessage) (CPUPayload, error) {
	var p CPUPayload
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	switch p.Operation {
	case "prime_count":
		if p.N < 2 || p.N > maxPrimeN {
			return p, errors.New("n must be between 2 and 20000000 for prime_count")
		}
	case "hash_chain":
		if p.N < 1 || p.N > maxHashN {
			return p, errors.New("n must be between 1 and 20000000 for hash_chain")
		}
		if len(p.Seed) > 1024 {
			return p, errors.New("seed must be at most 1024 bytes")
		}
	default:
		return p, errors.New(`operation must be "prime_count" or "hash_chain"`)
	}
	return p, nil
}

// Validate implements jobs.Handler.
func (c CPU) Validate(raw json.RawMessage) error { _, err := c.parse(raw); return err }

// Handle implements jobs.Handler.
func (c CPU) Handle(ctx context.Context, x jobs.Execution) (json.RawMessage, error) {
	p, err := c.parse(x.Payload)
	if err != nil {
		return nil, jobs.Permanent(err)
	}
	switch p.Operation {
	case "prime_count":
		n, err := CountPrimes(ctx, p.N)
		if err != nil {
			return nil, err
		}
		return mustJSON(map[string]any{"operation": p.Operation, "n": p.N, "prime_count": n}), nil
	default:
		h, err := HashChain(ctx, p.Seed, p.N)
		if err != nil {
			return nil, err
		}
		return mustJSON(map[string]any{"operation": p.Operation, "n": p.N, "digest": h}), nil
	}
}

// CountPrimes counts primes <= n with a sieve of Eratosthenes.
func CountPrimes(ctx context.Context, n int) (int, error) {
	composite := make([]bool, n+1)
	count := 0
	for i := 2; i <= n; i++ {
		if i&checkMask == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if composite[i] {
			continue
		}
		count++
		for j := i * i; j <= n && j > 0; j += i {
			composite[j] = true
		}
	}
	return count, nil
}

// HashChain applies SHA-256 n times starting from seed.
func HashChain(ctx context.Context, seed string, n int) (string, error) {
	sum := sha256.Sum256([]byte(seed))
	for i := 1; i < n; i++ {
		if i&checkMask == 0 && ctx.Err() != nil {
			return "", ctx.Err()
		}
		sum = sha256.Sum256(sum[:])
	}
	return hex.EncodeToString(sum[:]), nil
}

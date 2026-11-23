// Package auth implements API key authentication.
//
// Raw keys are never stored or logged: the database holds SHA-256 digests,
// and lookups are by digest. Keys are high-entropy random strings, so an
// unsalted fast hash is appropriate (unlike passwords).
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidKey is returned for unknown or revoked keys.
var ErrInvalidKey = errors.New("invalid API key")

// Principal identifies an authenticated caller.
type Principal struct {
	KeyID int64
	Name  string
}

// HashKey returns the SHA-256 digest of a raw key.
func HashKey(raw string) []byte {
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}

type cacheEntry struct {
	p       Principal
	expires time.Time
}

// Store validates API keys against the api_keys table with a short-lived
// positive cache so that revocations take effect within CacheTTL.
type Store struct {
	pool     *pgxpool.Pool
	cacheTTL time.Duration
	mu       sync.RWMutex
	cache    map[string]cacheEntry
}

// NewStore returns a Store.
func NewStore(pool *pgxpool.Pool, cacheTTL time.Duration) *Store {
	return &Store{pool: pool, cacheTTL: cacheTTL, cache: map[string]cacheEntry{}}
}

// Bootstrap ensures each configured key exists (idempotent). Keys are named
// by a digest prefix so they can be identified and revoked without ever
// revealing the key itself.
func (s *Store) Bootstrap(ctx context.Context, keys []string) error {
	for _, k := range keys {
		h := HashKey(k)
		name := "env-" + hex.EncodeToString(h[:6])
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO api_keys (name, key_hash) VALUES ($1, $2)
			ON CONFLICT (key_hash) DO NOTHING`, name, h); err != nil {
			return err
		}
	}
	return nil
}

// Authenticate resolves a raw key to a Principal.
func (s *Store) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, ErrInvalidKey
	}
	h := HashKey(raw)
	ck := string(h)
	s.mu.RLock()
	e, ok := s.cache[ck]
	s.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.p, nil
	}
	var p Principal
	err := s.pool.QueryRow(ctx, `SELECT id, name FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, h).
		Scan(&p.KeyID, &p.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		s.mu.Lock()
		delete(s.cache, ck)
		s.mu.Unlock()
		return Principal{}, ErrInvalidKey
	}
	if err != nil {
		return Principal{}, err
	}
	s.mu.Lock()
	if len(s.cache) > 10_000 { // bound memory; keys are few in practice
		s.cache = map[string]cacheEntry{}
	}
	s.cache[ck] = cacheEntry{p: p, expires: time.Now().Add(s.cacheTTL)}
	s.mu.Unlock()
	return p, nil
}

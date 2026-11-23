package worker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Info describes a worker process registration.
type Info struct {
	ID              string     `json:"id"`
	Hostname        string     `json:"hostname"`
	PID             int        `json:"pid"`
	Concurrency     int        `json:"concurrency"`
	Status          string     `json:"status"`
	Version         string     `json:"version"`
	StartedAt       time.Time  `json:"started_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	StoppedAt       *time.Time `json:"stopped_at,omitempty"`
}

// Registry persists worker registrations and heartbeats.
type Registry struct{ pool *pgxpool.Pool }

// NewRegistry returns a Registry.
func NewRegistry(pool *pgxpool.Pool) *Registry { return &Registry{pool: pool} }

// Register upserts the worker as active.
func (r *Registry) Register(ctx context.Context, w Info) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, pid, concurrency, status, version)
		VALUES ($1, $2, $3, $4, 'active', $5)
		ON CONFLICT (id) DO UPDATE SET hostname = EXCLUDED.hostname, pid = EXCLUDED.pid,
			concurrency = EXCLUDED.concurrency, status = 'active', version = EXCLUDED.version,
			started_at = now(), last_heartbeat_at = now(), stopped_at = NULL`,
		w.ID, w.Hostname, w.PID, w.Concurrency, w.Version)
	return err
}

// Heartbeat refreshes the worker's liveness timestamp. A worker previously
// declared dead (e.g. after a long GC pause or partition) becomes active
// again; its job leases are handled independently.
func (r *Registry) Heartbeat(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE workers SET last_heartbeat_at = now(),
			status = CASE WHEN status = 'dead' THEN 'active' ELSE status END
		WHERE id = $1`, id)
	return err
}

// SetStatus updates the worker status ("stopping" or "stopped").
func (r *Registry) SetStatus(ctx context.Context, id, status string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE workers SET status = $2, last_heartbeat_at = now(),
			stopped_at = CASE WHEN $2 = 'stopped' THEN now() ELSE stopped_at END
		WHERE id = $1`, id, status)
	return err
}

// MarkDead flags workers whose heartbeat is older than after.
func (r *Registry) MarkDead(ctx context.Context, after time.Duration) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE workers SET status = 'dead'
		WHERE status IN ('active', 'stopping') AND last_heartbeat_at < now() - ($1 * interval '1 millisecond')`,
		after.Milliseconds())
	return tag.RowsAffected(), err
}

// CountActive counts workers with a heartbeat newer than after.
func (r *Registry) CountActive(ctx context.Context, after time.Duration) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx, `
		SELECT count(*) FROM workers
		WHERE status IN ('active', 'stopping') AND last_heartbeat_at >= now() - ($1 * interval '1 millisecond')`,
		after.Milliseconds()).Scan(&n)
	return n, err
}

// List returns recently seen workers (newest heartbeat first).
func (r *Registry) List(ctx context.Context, limit int) ([]Info, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, hostname, pid, concurrency, status, version, started_at, last_heartbeat_at, stopped_at
		FROM workers ORDER BY last_heartbeat_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Info{}
	for rows.Next() {
		var w Info
		if err := rows.Scan(&w.ID, &w.Hostname, &w.PID, &w.Concurrency, &w.Status, &w.Version,
			&w.StartedAt, &w.LastHeartbeatAt, &w.StoppedAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

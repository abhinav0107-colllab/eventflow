package database

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is an arbitrary constant used with pg_advisory_lock so that
// concurrent migrators (e.g. several replicas starting at once) serialise.
const migrationLockID = 7_351_902_114

var migrationName = regexp.MustCompile(`^(\d{4,})_([a-z0-9_]+)\.(up|down)\.sql$`)

// Migration is one versioned schema change.
type Migration struct {
	Version int64
	Name    string
	Up      string
	Down    string
}

// MigrationStatus describes whether a migration has been applied.
type MigrationStatus struct {
	Migration
	AppliedAt *time.Time
}

// Migrator applies embedded SQL migrations. Each migration runs in its own
// transaction together with its bookkeeping row, so a failed migration leaves
// no partial state behind.
type Migrator struct {
	pool       *pgxpool.Pool
	migrations []Migration
}

// NewMigrator loads migrations from fsys.
func NewMigrator(pool *pgxpool.Pool, fsys fs.FS) (*Migrator, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	byVersion := map[int64]*Migration{}
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		v, _ := strconv.ParseInt(m[1], 10, 64)
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		mig := byVersion[v]
		if mig == nil {
			mig = &Migration{Version: v, Name: m[2]}
			byVersion[v] = mig
		} else if mig.Name != m[2] {
			return nil, fmt.Errorf("migration %d has conflicting names %q and %q", v, mig.Name, m[2])
		}
		if m[3] == "up" {
			mig.Up = string(body)
		} else {
			mig.Down = string(body)
		}
	}
	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.Up == "" || m.Down == "" {
			return nil, fmt.Errorf("migration %04d_%s must have both up and down files", m.Version, m.Name)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return &Migrator{pool: pool, migrations: out}, nil
}

func (m *Migrator) withLock(ctx context.Context, fn func(conn *pgxpool.Conn) error) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    BIGINT PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return fn(conn)
}

func applied(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) (map[int64]time.Time, error) {
	rows, err := q.Query(ctx, `SELECT version, applied_at FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	out := map[int64]time.Time{}
	for rows.Next() {
		var v int64
		var t time.Time
		if err := rows.Scan(&v, &t); err != nil {
			return nil, err
		}
		out[v] = t
	}
	return out, rows.Err()
}

// Up applies all pending migrations and returns the versions applied.
func (m *Migrator) Up(ctx context.Context) ([]int64, error) {
	var done []int64
	err := m.withLock(ctx, func(conn *pgxpool.Conn) error {
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}
		for _, mig := range m.migrations {
			if _, ok := have[mig.Version]; ok {
				continue
			}
			err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, mig.Up); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, mig.Version, mig.Name)
				return err
			})
			if err != nil {
				return fmt.Errorf("apply migration %04d_%s: %w", mig.Version, mig.Name, err)
			}
			done = append(done, mig.Version)
		}
		return nil
	})
	return done, err
}

// Down rolls back the most recent `steps` applied migrations.
func (m *Migrator) Down(ctx context.Context, steps int) ([]int64, error) {
	if steps < 1 {
		return nil, errors.New("steps must be >= 1")
	}
	var done []int64
	err := m.withLock(ctx, func(conn *pgxpool.Conn) error {
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}
		for i := len(m.migrations) - 1; i >= 0 && len(done) < steps; i-- {
			mig := m.migrations[i]
			if _, ok := have[mig.Version]; !ok {
				continue
			}
			err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, mig.Down); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, mig.Version)
				return err
			})
			if err != nil {
				return fmt.Errorf("roll back migration %04d_%s: %w", mig.Version, mig.Name, err)
			}
			done = append(done, mig.Version)
		}
		return nil
	})
	return done, err
}

// Status lists every known migration with its applied timestamp, if any.
func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
	var out []MigrationStatus
	err := m.withLock(ctx, func(conn *pgxpool.Conn) error {
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}
		for _, mig := range m.migrations {
			st := MigrationStatus{Migration: mig}
			if t, ok := have[mig.Version]; ok {
				t := t
				st.AppliedAt = &t
			}
			out = append(out, st)
		}
		return nil
	})
	return out, err
}

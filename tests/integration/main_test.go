//go:build integration

// Package integration contains end-to-end tests that run the real EventFlow
// components against real PostgreSQL and Redis containers (Testcontainers).
// Nothing in this package mocks the database or the broker.
//
// Run with: go test -tags integration -count=1 ./tests/integration/...
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/migrations"
)

// Shared infrastructure for the whole package.
var (
	pgURL        string // superuser DSN for the test database
	redisAddr    string // host:port
	workerBinary string // compiled cmd/worker, used for real SIGKILL crash tests
	repoRoot     string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, file, _, _ := runtime.Caller(0)
	repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("eventflow_test"),
		tcpostgres.WithUsername("eventflow"),
		tcpostgres.WithPassword("test-only-password"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = pg.Terminate(context.Background()) }()
	pgURL, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}

	rc, err := tcredis.Run(ctx, "redis:7.2-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redis:", err)
		return 1
	}
	defer func() { _ = rc.Terminate(context.Background()) }()
	host, err := rc.Host(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redis host:", err)
		return 1
	}
	port, err := rc.MappedPort(ctx, "6379/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "redis port:", err)
		return 1
	}
	redisAddr = fmt.Sprintf("%s:%s", host, port.Port())

	pool, err := database.Connect(ctx, database.PoolConfig{URL: pgURL, MaxConns: 2})
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		return 1
	}
	mig, err := database.NewMigrator(pool, migrations.FS)
	if err == nil {
		_, err = mig.Up(ctx)
	}
	pool.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}

	binDir, err := os.MkdirTemp("", "eventflow-it-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(binDir)
	workerBinary = filepath.Join(binDir, "worker")
	build := exec.CommandContext(ctx, "go", "build", "-o", workerBinary, "./cmd/worker")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build worker: %v\n%s", err, out)
		return 1
	}
	return m.Run()
}

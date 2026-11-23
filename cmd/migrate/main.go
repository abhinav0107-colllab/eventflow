// Command migrate applies or rolls back the embedded database migrations.
//
//	migrate up            apply all pending migrations
//	migrate down [N]      roll back the last N migrations (default 1)
//	migrate status        list migrations and whether they are applied
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/config"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: migrate up | down [N] | status")
	}
	cfg, err := config.LoadCommon()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, err := database.ConnectWithRetry(ctx, database.PoolConfig{URL: cfg.DatabaseURL, MaxConns: 2}, 30, 2*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()
	m, err := database.NewMigrator(pool, migrations.FS)
	if err != nil {
		return err
	}
	switch args[0] {
	case "up":
		done, err := m.Up(ctx)
		for _, v := range done {
			fmt.Printf("applied %04d\n", v)
		}
		if err == nil && len(done) == 0 {
			fmt.Println("schema is up to date")
		}
		return err
	case "down":
		steps := 1
		if len(args) > 1 {
			if steps, err = strconv.Atoi(args[1]); err != nil || steps < 1 {
				return errors.New("down: N must be a positive integer")
			}
		}
		done, err := m.Down(ctx, steps)
		for _, v := range done {
			fmt.Printf("rolled back %04d\n", v)
		}
		return err
	case "status":
		st, err := m.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range st {
			applied := "pending"
			if s.AppliedAt != nil {
				applied = "applied " + s.AppliedAt.UTC().Format(time.RFC3339)
			}
			fmt.Printf("%04d_%s\t%s\n", s.Version, s.Name, applied)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

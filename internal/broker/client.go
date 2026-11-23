package broker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClientConfig configures the Redis connection.
type ClientConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

// NewClient returns a Redis client. Read timeouts must exceed the longest
// XREADGROUP BLOCK duration; go-redis extends the deadline for blocking
// commands automatically.
func NewClient(cfg ClientConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   2,
	})
}

// WaitReady pings Redis until it answers or ctx expires.
func WaitReady(ctx context.Context, rdb redis.UniversalClient, attempts int, delay time.Duration) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

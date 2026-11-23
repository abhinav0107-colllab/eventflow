// Package config loads process configuration from environment variables.
//
// Every setting has a safe default for local development except secrets,
// which must be supplied explicitly. Values are validated at start-up so that
// misconfiguration fails fast instead of at first use.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "EVENTFLOW_"

// Common settings shared by every binary.
type Common struct {
	DatabaseURL     string
	DBMaxConns      int32
	DBMinConns      int32
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	StreamPrefix    string
	ConsumerGroup   string
	LogLevel        string
	ShutdownTimeout time.Duration
}

// Retry configures exponential backoff for failed attempts.
type Retry struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// API settings for cmd/api.
type API struct {
	Common
	HTTPAddr            string
	APIKeys             []string
	MaxBodyBytes        int64
	MaxPayloadBytes     int
	DefaultMaxAttempts  int
	DefaultTimeout      time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	OutboxEnabled       bool
	OutboxPollInterval  time.Duration
	OutboxBatchSize     int
	OutboxRetention     time.Duration
	StatsInterval       time.Duration
	WorkerDeadAfter     time.Duration
	PublishRetry        Retry
	TrustForwardedProto bool
}

// Worker settings for cmd/worker.
type Worker struct {
	Common
	WorkerID          string
	Concurrency       int
	MetricsAddr       string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	BlockTimeout      time.Duration
	ClaimInterval     time.Duration
	ClaimMinIdle      time.Duration
	RecoveryInterval  time.Duration
	StuckQueuedAfter  time.Duration
	WorkerDeadAfter   time.Duration
	Retry             Retry
	OutboxEnabled     bool
	OutboxPoll        time.Duration
	OutboxBatchSize   int
}

type loader struct {
	errs []error
	get  func(string) (string, bool)
}

func newLoader() *loader { return &loader{get: os.LookupEnv} }

func (l *loader) str(name, def string) string {
	if v, ok := l.get(envPrefix + name); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) required(name string) string {
	v := l.str(name, "")
	if v == "" {
		l.errs = append(l.errs, fmt.Errorf("%s%s is required", envPrefix, name))
	}
	return v
}

func (l *loader) integer(name string, def, min, max int) int {
	raw := l.str(name, "")
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < min || v > max {
		l.errs = append(l.errs, fmt.Errorf("%s%s must be an integer in [%d, %d]", envPrefix, name, min, max))
		return def
	}
	return v
}

func (l *loader) duration(name string, def, min time.Duration) time.Duration {
	raw := l.str(name, "")
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v < min {
		l.errs = append(l.errs, fmt.Errorf("%s%s must be a duration >= %s (e.g. 30s)", envPrefix, name, min))
		return def
	}
	return v
}

func (l *loader) boolean(name string, def bool) bool {
	raw := l.str(name, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s%s must be a boolean", envPrefix, name))
		return def
	}
	return v
}

func (l *loader) common() Common {
	c := Common{
		DatabaseURL:     l.required("DATABASE_URL"),
		DBMaxConns:      int32(l.integer("DB_MAX_CONNS", 20, 1, 1000)),
		DBMinConns:      int32(l.integer("DB_MIN_CONNS", 2, 0, 1000)),
		RedisAddr:       l.str("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   l.str("REDIS_PASSWORD", ""),
		RedisDB:         l.integer("REDIS_DB", 0, 0, 15),
		StreamPrefix:    l.str("STREAM_PREFIX", "eventflow"),
		ConsumerGroup:   l.str("CONSUMER_GROUP", "eventflow-workers"),
		LogLevel:        strings.ToLower(l.str("LOG_LEVEL", "info")),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 30*time.Second, time.Second),
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		l.errs = append(l.errs, fmt.Errorf("%sLOG_LEVEL must be one of debug, info, warn, error", envPrefix))
	}
	if c.DBMinConns > c.DBMaxConns {
		l.errs = append(l.errs, fmt.Errorf("%sDB_MIN_CONNS must not exceed %sDB_MAX_CONNS", envPrefix, envPrefix))
	}
	return c
}

// LoadCommon loads only the shared settings (used by cmd/migrate).
func LoadCommon() (Common, error) {
	l := newLoader()
	c := l.common()
	return c, errors.Join(l.errs...)
}

// LoadAPI loads and validates API server settings.
func LoadAPI() (API, error) {
	l := newLoader()
	c := API{
		Common:             l.common(),
		HTTPAddr:           l.str("HTTP_ADDR", ":8080"),
		MaxBodyBytes:       int64(l.integer("MAX_BODY_BYTES", 1<<20, 1024, 64<<20)),
		MaxPayloadBytes:    l.integer("MAX_PAYLOAD_BYTES", 256<<10, 64, 16<<20),
		DefaultMaxAttempts: l.integer("DEFAULT_MAX_ATTEMPTS", 3, 1, 1000),
		DefaultTimeout:     l.duration("DEFAULT_TIMEOUT", 30*time.Second, time.Second),
		ReadTimeout:        l.duration("HTTP_READ_TIMEOUT", 15*time.Second, time.Second),
		WriteTimeout:       l.duration("HTTP_WRITE_TIMEOUT", 30*time.Second, time.Second),
		OutboxEnabled:      l.boolean("OUTBOX_ENABLED", true),
		OutboxPollInterval: l.duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond, 10*time.Millisecond),
		OutboxBatchSize:    l.integer("OUTBOX_BATCH_SIZE", 200, 1, 5000),
		OutboxRetention:    l.duration("OUTBOX_RETENTION", 72*time.Hour, time.Minute),
		StatsInterval:      l.duration("STATS_INTERVAL", 5*time.Second, 100*time.Millisecond),
		WorkerDeadAfter:    l.duration("WORKER_DEAD_AFTER", 45*time.Second, time.Second),
		PublishRetry: Retry{
			BaseDelay: l.duration("OUTBOX_RETRY_BASE_DELAY", 500*time.Millisecond, time.Millisecond),
			MaxDelay:  l.duration("OUTBOX_RETRY_MAX_DELAY", 30*time.Second, time.Millisecond),
		},
	}
	for _, k := range strings.Split(l.str("API_KEYS", ""), ",") {
		if k = strings.TrimSpace(k); k == "" {
			continue
		}
		if len(k) < 16 {
			l.errs = append(l.errs, fmt.Errorf("%sAPI_KEYS entries must be at least 16 characters", envPrefix))
			continue
		}
		c.APIKeys = append(c.APIKeys, k)
	}
	if len(c.APIKeys) == 0 {
		l.errs = append(l.errs, fmt.Errorf("%sAPI_KEYS must contain at least one key", envPrefix))
	}
	if int64(c.MaxPayloadBytes) >= c.MaxBodyBytes {
		l.errs = append(l.errs, fmt.Errorf("%sMAX_PAYLOAD_BYTES must be smaller than %sMAX_BODY_BYTES", envPrefix, envPrefix))
	}
	return c, errors.Join(l.errs...)
}

// LoadWorker loads and validates worker settings.
func LoadWorker() (Worker, error) {
	l := newLoader()
	c := Worker{
		Common:            l.common(),
		WorkerID:          l.str("WORKER_ID", ""),
		Concurrency:       l.integer("WORKER_CONCURRENCY", 8, 1, 1024),
		MetricsAddr:       l.str("WORKER_METRICS_ADDR", ":9091"),
		LeaseDuration:     l.duration("LEASE_DURATION", 30*time.Second, time.Second),
		HeartbeatInterval: l.duration("HEARTBEAT_INTERVAL", 10*time.Second, 100*time.Millisecond),
		BlockTimeout:      l.duration("BLOCK_TIMEOUT", 2*time.Second, 10*time.Millisecond),
		ClaimInterval:     l.duration("CLAIM_INTERVAL", 15*time.Second, 100*time.Millisecond),
		ClaimMinIdle:      l.duration("CLAIM_MIN_IDLE", 60*time.Second, 100*time.Millisecond),
		RecoveryInterval:  l.duration("RECOVERY_INTERVAL", 5*time.Second, 100*time.Millisecond),
		StuckQueuedAfter:  l.duration("STUCK_QUEUED_AFTER", 2*time.Minute, time.Second),
		WorkerDeadAfter:   l.duration("WORKER_DEAD_AFTER", 45*time.Second, time.Second),
		Retry: Retry{
			BaseDelay: l.duration("RETRY_BASE_DELAY", 2*time.Second, time.Millisecond),
			MaxDelay:  l.duration("RETRY_MAX_DELAY", 10*time.Minute, time.Millisecond),
		},
		OutboxEnabled:   l.boolean("WORKER_OUTBOX_ENABLED", false),
		OutboxPoll:      l.duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond, 10*time.Millisecond),
		OutboxBatchSize: l.integer("OUTBOX_BATCH_SIZE", 200, 1, 5000),
	}
	if c.HeartbeatInterval*2 > c.LeaseDuration {
		l.errs = append(l.errs, fmt.Errorf("%sHEARTBEAT_INTERVAL must be at most half of %sLEASE_DURATION", envPrefix, envPrefix))
	}
	if c.Retry.BaseDelay > c.Retry.MaxDelay {
		l.errs = append(l.errs, fmt.Errorf("%sRETRY_BASE_DELAY must not exceed %sRETRY_MAX_DELAY", envPrefix, envPrefix))
	}
	return c, errors.Join(l.errs...)
}

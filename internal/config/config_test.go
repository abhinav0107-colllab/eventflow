package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadAPIRequiresSecrets(t *testing.T) {
	t.Setenv("EVENTFLOW_DATABASE_URL", "")
	t.Setenv("EVENTFLOW_API_KEYS", "")
	_, err := LoadAPI()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"EVENTFLOW_DATABASE_URL is required", "EVENTFLOW_API_KEYS must contain"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestLoadAPIDefaultsAndOverrides(t *testing.T) {
	t.Setenv("EVENTFLOW_DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("EVENTFLOW_API_KEYS", " key-aaaaaaaaaaaaaaaa , ,key-bbbbbbbbbbbbbbbb")
	t.Setenv("EVENTFLOW_HTTP_ADDR", ":9999")
	t.Setenv("EVENTFLOW_OUTBOX_POLL_INTERVAL", "50ms")
	c, err := LoadAPI()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.APIKeys) != 2 || c.HTTPAddr != ":9999" || c.OutboxPollInterval != 50*time.Millisecond {
		t.Fatalf("unexpected config: %+v", c)
	}
	if c.DefaultMaxAttempts != 3 || c.MaxBodyBytes != 1<<20 || c.LogLevel != "info" || !c.OutboxEnabled {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	t.Setenv("EVENTFLOW_DATABASE_URL", "postgres://x")
	t.Setenv("EVENTFLOW_API_KEYS", "short")
	t.Setenv("EVENTFLOW_DB_MAX_CONNS", "zero")
	t.Setenv("EVENTFLOW_LOG_LEVEL", "verbose")
	t.Setenv("EVENTFLOW_DEFAULT_TIMEOUT", "5")
	_, err := LoadAPI()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"API_KEYS entries must be at least 16", "DB_MAX_CONNS", "LOG_LEVEL", "DEFAULT_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}

func TestLoadWorkerChecksLeaseVsHeartbeat(t *testing.T) {
	t.Setenv("EVENTFLOW_DATABASE_URL", "postgres://x")
	t.Setenv("EVENTFLOW_LEASE_DURATION", "10s")
	t.Setenv("EVENTFLOW_HEARTBEAT_INTERVAL", "8s")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "HEARTBEAT_INTERVAL") {
		t.Fatalf("expected heartbeat/lease error, got %v", err)
	}
	t.Setenv("EVENTFLOW_HEARTBEAT_INTERVAL", "2s")
	c, err := LoadWorker()
	if err != nil || c.Concurrency != 8 || c.LeaseDuration != 10*time.Second {
		t.Fatalf("unexpected: %+v %v", c, err)
	}
}

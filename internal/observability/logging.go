// Package observability provides structured logging and Prometheus metrics.
package observability

import (
	"io"
	"log/slog"
	"os"
)

// NewLogger returns a JSON slog logger tagged with the service name.
func NewLogger(service, level string) *slog.Logger {
	return newLogger(os.Stdout, service, level)
}

func newLogger(w io.Writer, service, level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lv})
	return slog.New(h).With("service", service)
}

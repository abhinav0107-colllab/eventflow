package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Label cardinality is bounded by design: job types come from the handler
// registry, queues and error classes are fixed enums, and HTTP routes are
// the registered patterns (never raw paths). Job IDs are never labels.

// Metrics holds every EventFlow collector. Each process registers the full
// set; series only appear once a process records a value.
type Metrics struct {
	Registry *prometheus.Registry

	JobsSubmitted       *prometheus.CounterVec
	JobsSucceeded       *prometheus.CounterVec
	AttemptFailures     *prometheus.CounterVec
	JobsRetried         *prometheus.CounterVec
	JobsDeadLettered    *prometheus.CounterVec
	JobsCancelled       *prometheus.CounterVec
	JobDuration         *prometheus.HistogramVec
	QueueWait           *prometheus.HistogramVec
	DuplicateDeliveries prometheus.Counter
	WorkerFailures      *prometheus.CounterVec
	WorkerInflight      prometheus.Gauge
	WorkerCapacity      prometheus.Gauge
	RecoveredJobs       *prometheus.CounterVec

	OutboxPublished      *prometheus.CounterVec
	OutboxPublishErrors  prometheus.Counter
	OutboxPublishLatency prometheus.Histogram

	QueueDepth      *prometheus.GaugeVec
	QueuePending    *prometheus.GaugeVec
	JobsInState     *prometheus.GaugeVec
	DeadLettered    prometheus.Gauge
	OutboxBacklog   prometheus.Gauge
	OutboxOldestAge prometheus.Gauge
	ActiveWorkers   prometheus.Gauge

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
}

const ns = "eventflow"

// NewMetrics creates and registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	durBuckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

	m := &Metrics{Registry: r}
	m.JobsSubmitted = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_submitted_total",
		Help: "Jobs accepted by the API (idempotent replays excluded)."}, []string{"type"})
	m.JobsSucceeded = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_succeeded_total",
		Help: "Jobs that completed successfully."}, []string{"type"})
	m.AttemptFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "job_attempt_failures_total",
		Help: "Failed execution attempts by error class."}, []string{"type", "class"})
	m.JobsRetried = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_retried_total",
		Help: "Failed attempts that were scheduled for automatic retry."}, []string{"type"})
	m.JobsDeadLettered = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_dead_lettered_total",
		Help: "Jobs that failed terminally and were dead-lettered."}, []string{"type"})
	m.JobsCancelled = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "jobs_cancelled_total",
		Help: "Jobs cancelled while running."}, []string{"type"})
	m.JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "job_execution_duration_seconds",
		Help: "Handler execution time per attempt.", Buckets: durBuckets}, []string{"type", "outcome"})
	m.QueueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "job_queue_wait_seconds",
		Help: "Time from a job becoming runnable (scheduled_at) to an attempt starting.", Buckets: durBuckets}, []string{"queue"})
	m.DuplicateDeliveries = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "duplicate_deliveries_total",
		Help: "Delivered messages dropped because the job was not claimable (duplicate, stale or cancelled)."})
	m.WorkerFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "worker_failures_total",
		Help: "Worker-side infrastructure failures by reason."}, []string{"reason"})
	m.WorkerInflight = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "worker_inflight_jobs",
		Help: "Jobs currently executing in this worker process."})
	m.WorkerCapacity = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "worker_concurrency",
		Help: "Configured concurrency of this worker process."})
	m.RecoveredJobs = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "recovered_jobs_total",
		Help: "Jobs recovered by the recovery manager."}, []string{"reason"})

	m.OutboxPublished = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "outbox_published_total",
		Help: "Outbox events published to Redis."}, []string{"event_type"})
	m.OutboxPublishErrors = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "outbox_publish_errors_total",
		Help: "Failed outbox publication attempts."})
	m.OutboxPublishLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "outbox_publish_delay_seconds",
		Help: "Delay between an outbox event becoming available and being published.", Buckets: durBuckets})

	m.QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "queue_depth",
		Help: "Messages awaiting completion in a stream (undelivered + pending)."}, []string{"queue"})
	m.QueuePending = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "queue_pending_messages",
		Help: "Delivered but unacknowledged messages (consumer group PEL)."}, []string{"queue"})
	m.JobsInState = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "jobs_in_state",
		Help: "Non-terminal jobs by queue and status (from PostgreSQL)."}, []string{"queue", "status"})
	m.DeadLettered = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "dead_letter_jobs",
		Help: "Jobs currently in the dead-letter state."})
	m.OutboxBacklog = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "outbox_backlog",
		Help: "Outbox events that are due but not yet published."})
	m.OutboxOldestAge = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "outbox_oldest_due_age_seconds",
		Help: "Age of the oldest due, unpublished outbox event."})
	m.ActiveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "active_workers",
		Help: "Workers with a recent heartbeat."})

	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "http_requests_total",
		Help: "HTTP requests by route pattern and status code."}, []string{"method", "route", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern.",
		Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}}, []string{"method", "route"})

	for _, c := range []prometheus.Collector{
		m.JobsSubmitted, m.JobsSucceeded, m.AttemptFailures, m.JobsRetried, m.JobsDeadLettered, m.JobsCancelled,
		m.JobDuration, m.QueueWait, m.DuplicateDeliveries, m.WorkerFailures, m.WorkerInflight, m.WorkerCapacity,
		m.RecoveredJobs, m.OutboxPublished, m.OutboxPublishErrors, m.OutboxPublishLatency, m.QueueDepth,
		m.QueuePending, m.JobsInState, m.DeadLettered, m.OutboxBacklog, m.OutboxOldestAge, m.ActiveWorkers,
		m.HTTPRequests, m.HTTPDuration,
	} {
		f(c)
	}
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

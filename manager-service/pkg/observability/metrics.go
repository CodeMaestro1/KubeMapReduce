package observability

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the registered set of Prometheus collectors exposed by the
// Manager and API services.
//
// All counters and histograms are registered against [prometheus.DefaultRegisterer]
// so the default [promhttp.Handler] surfaces them without further wiring. The
// struct is constructed once at process startup via [NewMetrics] and shared
// (read-only) between goroutines; the underlying client_golang collectors are
// safe for concurrent use.
type Metrics struct {
	// TasksScheduled counts every task the scheduler persists during
	// ScheduleJob, partitioned by task_type ("Map" | "Reduce").
	TasksScheduled *prometheus.CounterVec
	// TasksCompleted counts successful TaskComplete RPCs.
	TasksCompleted prometheus.Counter
	// TasksFailed counts TaskFailed RPCs and reaper-driven failures.
	TasksFailed prometheus.Counter
	// HeartbeatsTotal counts every heartbeat received from a worker,
	// partitioned by action ("CONTINUE" | "TERMINATE").
	HeartbeatsTotal *prometheus.CounterVec
	// ReaperRecovered counts stale task attempts reclaimed by the active
	// reaper. Each label set value represents one reaped attempt.
	ReaperRecovered prometheus.Counter
	// SchedulerCycleSeconds is the duration of a single FailStaleTasks
	// reaper cycle, regardless of whether any tasks were recovered.
	SchedulerCycleSeconds prometheus.Histogram
	// HTTPRequestDurationSeconds is the duration of inbound HTTP requests
	// handled by the API/Manager mux, partitioned by method and status
	// code class. Populated by [RequestIDMiddleware] when a non-nil
	// Metrics value is configured.
	HTTPRequestDurationSeconds *prometheus.HistogramVec
}

// NewMetrics constructs and registers the Prometheus collectors used across
// the platform. The returned [*Metrics] is safe for concurrent use; passing
// the same instance into multiple subsystems (scheduler, gRPC server, HTTP
// middleware) is the intended pattern.
//
// Registration uses [prometheus.MustRegister], which panics on duplicate
// registration; tests that need an isolated registry should construct a
// dedicated [prometheus.Registry] and instantiate the collectors directly
// instead of calling this helper twice.
func NewMetrics() *Metrics {
	m := &Metrics{
		TasksScheduled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubemapreduce_tasks_scheduled_total",
			Help: "Total number of tasks persisted by the scheduler, partitioned by task_type.",
		}, []string{"task_type"}),
		TasksCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kubemapreduce_tasks_completed_total",
			Help: "Total number of task attempts that committed successfully.",
		}),
		TasksFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kubemapreduce_tasks_failed_total",
			Help: "Total number of task attempts that failed (worker-reported or reaper-driven).",
		}),
		HeartbeatsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubemapreduce_heartbeats_total",
			Help: "Total number of worker heartbeats received, partitioned by manager-issued action.",
		}, []string{"action"}),
		ReaperRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kubemapreduce_reaper_recovered_total",
			Help: "Total number of stale task attempts reclaimed by the active reaper.",
		}),
		SchedulerCycleSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kubemapreduce_reaper_cycle_seconds",
			Help:    "Duration of a single FailStaleTasks reaper cycle in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		HTTPRequestDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kubemapreduce_http_request_duration_seconds",
			Help:    "Duration of inbound HTTP requests in seconds, partitioned by method and status class.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "status_class"}),
	}
	prometheus.MustRegister(
		m.TasksScheduled,
		m.TasksCompleted,
		m.TasksFailed,
		m.HeartbeatsTotal,
		m.ReaperRecovered,
		m.SchedulerCycleSeconds,
		m.HTTPRequestDurationSeconds,
	)
	return m
}

// MetricsHandler returns an [http.Handler] that serves the Prometheus
// exposition format from [prometheus.DefaultGatherer]. Mount it at /metrics
// on every service that calls [NewMetrics].
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

var (
	defaultMetrics   *Metrics
	defaultMetricsMu sync.RWMutex
)

// SetDefaultMetrics installs m as the package-level metrics singleton accessed
// by [DefaultMetrics]. It is called once during service bootstrap so internal
// packages can record observations without the Scheduler/WorkerServer
// constructors having to grow a new parameter. Passing nil disables recording
// (useful for tests).
func SetDefaultMetrics(m *Metrics) {
	defaultMetricsMu.Lock()
	defer defaultMetricsMu.Unlock()
	defaultMetrics = m
}

// DefaultMetrics returns the singleton previously installed via
// [SetDefaultMetrics], or nil if metrics have not been initialised. Callers
// must always nil-check the return value before recording observations so
// unit tests that bypass bootstrap remain side-effect free.
func DefaultMetrics() *Metrics {
	defaultMetricsMu.RLock()
	defer defaultMetricsMu.RUnlock()
	return defaultMetrics
}

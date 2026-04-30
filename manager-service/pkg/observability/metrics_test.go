package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// withTemporaryRegistry swaps DefaultRegisterer/Gatherer for the duration of
// a test so multiple NewMetrics() calls in the same binary do not collide on
// the global registry.
func withTemporaryRegistry(t *testing.T) {
	t.Helper()
	prevReg := prometheus.DefaultRegisterer
	prevGath := prometheus.DefaultGatherer
	r := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = r
	prometheus.DefaultGatherer = r
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = prevReg
		prometheus.DefaultGatherer = prevGath
		SetDefaultMetrics(nil)
	})
}

func TestNewMetrics_RegistersAndIncrements(t *testing.T) {
	withTemporaryRegistry(t)

	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
	m.TasksScheduled.WithLabelValues("Map").Inc()
	m.TasksCompleted.Inc()
	m.TasksFailed.Inc()
	m.HeartbeatsTotal.WithLabelValues("CONTINUE").Inc()
	m.ReaperRecovered.Add(2)
	m.SchedulerCycleSeconds.Observe(0.123)
	m.HTTPRequestDurationSeconds.WithLabelValues("GET", "2xx").Observe(0.05)
}

func TestMetricsHandler_ExposesPrometheusFormat(t *testing.T) {
	withTemporaryRegistry(t)

	m := NewMetrics()
	SetDefaultMetrics(m)
	// Vec collectors only expose a sample after a label set has been
	// observed at least once, so prime each one before scraping.
	m.TasksScheduled.WithLabelValues("Map").Inc()
	m.TasksCompleted.Inc()
	m.TasksFailed.Inc()
	m.HeartbeatsTotal.WithLabelValues("CONTINUE").Inc()
	m.ReaperRecovered.Inc()
	m.SchedulerCycleSeconds.Observe(0.1)
	m.HTTPRequestDurationSeconds.WithLabelValues("GET", "2xx").Observe(0.05)

	srv := httptest.NewServer(MetricsHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	got := string(body)
	for _, want := range []string{
		"kubemapreduce_tasks_scheduled_total",
		"kubemapreduce_tasks_completed_total",
		"kubemapreduce_tasks_failed_total",
		"kubemapreduce_heartbeats_total",
		"kubemapreduce_reaper_recovered_total",
		"kubemapreduce_reaper_cycle_seconds",
		"kubemapreduce_http_request_duration_seconds",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected /metrics output to contain %q", want)
		}
	}
}

func TestDefaultMetrics_NilByDefault(t *testing.T) {
	withTemporaryRegistry(t)
	if got := DefaultMetrics(); got != nil {
		t.Fatalf("expected DefaultMetrics() to be nil before SetDefaultMetrics, got %v", got)
	}
}

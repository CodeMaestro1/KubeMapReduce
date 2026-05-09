package relay

import (
	"context"
	"testing"
	"time"

	"kubemapreduce/manager-service/internal/events"
	"kubemapreduce/manager-service/internal/store"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

type mockPublisher struct {
	published []*events.EventEnvelope
	failOn    map[uuid.UUID]bool
}

func (m *mockPublisher) Publish(ctx context.Context, event *events.EventEnvelope) error {
	if m.failOn[event.EventID] {
		return assertAnError("simulated publish failure")
	}
	m.published = append(m.published, event)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func assertAnError(msg string) error {
	return &testError{msg: msg}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestNewRelayService(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	os := store.NewOutboxStore(db)
	pub := &mockPublisher{}

	svc := NewRelayService(os, pub, RelayConfig{
		Enabled:    true,
		MaxRetries: 5,
		Interval:   100 * time.Millisecond,
	})

	if svc == nil {
		t.Fatal("NewRelayService returned nil")
	}
	if svc.maxRetries != 5 {
		t.Errorf("maxRetries = %d, want 5", svc.maxRetries)
	}
	if svc.interval != 100*time.Millisecond {
		t.Errorf("interval = %v, want 100ms", svc.interval)
	}
	if !svc.enabled {
		t.Errorf("enabled should be true")
	}
}

func TestNewRelayService_Defaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	os := store.NewOutboxStore(db)
	pub := &mockPublisher{}

	svc := NewRelayService(os, pub, RelayConfig{
		Enabled: true,
	})

	if svc.maxRetries != 3 {
		t.Errorf("default maxRetries = %d, want 3", svc.maxRetries)
	}
	if svc.interval != 5*time.Second {
		t.Errorf("default interval = %v, want 5s", svc.interval)
	}
}

func TestRelayService_Disabled(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	os := store.NewOutboxStore(db)
	pub := &mockPublisher{}

	svc := NewRelayService(os, pub, RelayConfig{
		Enabled: false,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	svc.Start(ctx)
	// Should not block or process anything
	time.Sleep(30 * time.Millisecond)
	if len(pub.published) != 0 {
		t.Errorf("expected no published events, got %d", len(pub.published))
	}
}

func TestNoopPublisher(t *testing.T) {
	pub := &NoopPublisher{}

	if err := pub.Publish(context.Background(), &events.EventEnvelope{}); err != nil {
		t.Errorf("NoopPublisher.Publish() returned error: %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Errorf("NoopPublisher.Close() returned error: %v", err)
	}
}

func TestEventToSubject(t *testing.T) {
	subject := EventToSubject(events.TaskAssigned)
	if subject != "tasks.assigned" {
		t.Errorf("EventToSubject = %q, want %q", subject, "tasks.assigned")
	}

	subject = EventToSubject(events.JobSubmitted)
	if subject != "jobs.submitted" {
		t.Errorf("EventToSubject = %q, want %q", subject, "jobs.submitted")
	}
}

func TestSubjectForEvent_TaskScopedPerJob(t *testing.T) {
	job := uuid.New()
	got := SubjectForEvent(&events.EventEnvelope{EventType: events.TaskAssigned, JobID: job})
	want := "tasks.assigned." + job.String()
	if got != want {
		t.Errorf("SubjectForEvent = %q, want %q", got, want)
	}
}

func TestSubjectForEvent_NonTaskUnchanged(t *testing.T) {
	got := SubjectForEvent(&events.EventEnvelope{EventType: events.JobSubmitted, JobID: uuid.New()})
	if got != "jobs.submitted" {
		t.Errorf("SubjectForEvent = %q, want %q", got, "jobs.submitted")
	}
}

func TestSubjectForEvent_TaskWithoutJobIDFallsBack(t *testing.T) {
	got := SubjectForEvent(&events.EventEnvelope{EventType: events.TaskAssigned, JobID: uuid.Nil})
	if got != "tasks.assigned" {
		t.Errorf("SubjectForEvent = %q, want %q", got, "tasks.assigned")
	}
}

func TestRelayService_StopIsIdempotent(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	svc := NewRelayService(store.NewOutboxStore(db), &NoopPublisher{}, RelayConfig{Enabled: false})
	svc.Stop()
	svc.Stop() // must not panic
}

// TestRelayService_ProcessBatch_BatchedCommitNoPerEventSleep verifies that a
// successful batch performs exactly one Begin / one Commit and invokes the
// metrics hooks once per event without any per-event sleep (review M1).
func TestRelayService_ProcessBatch_BatchedCommitNoPerEventSleep(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	eventID := uuid.New()
	jobID := uuid.New()
	emittedAt := time.Now()

	rows := sqlmock.NewRows([]string{
		"event_id", "event_type", "aggregate_type", "aggregate_id", "job_id",
		"attempt_id", "lease_id", "sequence", "emitted_at", "producer",
		"schema_version", "payload", "retry_count",
	}).AddRow(
		eventID.String(), "tasks.assigned", "task", uuid.New().String(), jobID.String(),
		nil, nil, int64(1), emittedAt, "manager",
		1, []byte(`{}`), 0,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(`FROM EVENT_OUTBOX`).
		WithArgs("events.deadletter", 100).
		WillReturnRows(rows)
	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET delivered = TRUE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	var (
		latencyCalls int
		retryCalls   int
		dlCalls      int
		pubCalls     int
	)
	hook := &MetricsHook{
		ObservePublishLatency: func(string, float64) { latencyCalls++ },
		IncRetry:              func(string) { retryCalls++ },
		IncDeadLetter:         func(string) { dlCalls++ },
		IncPublishedTotal:     func(string, bool) { pubCalls++ },
	}
	svc := NewRelayService(store.NewOutboxStore(db), &NoopPublisher{}, RelayConfig{
		Enabled:    true,
		MaxRetries: 3,
		BatchSize:  100,
		Metrics:    hook,
	})

	start := time.Now()
	if err := svc.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 250*time.Millisecond {
		t.Errorf("processBatch took %v; per-event sleep suspected", elapsed)
	}
	if latencyCalls != 1 {
		t.Errorf("ObservePublishLatency calls = %d, want 1", latencyCalls)
	}
	if retryCalls != 0 || dlCalls != 0 {
		t.Errorf("expected no retry/dead-letter calls; got retry=%d dl=%d", retryCalls, dlCalls)
	}
	if pubCalls != 1 {
		t.Errorf("IncPublishedTotal calls = %d, want 1", pubCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRelayService_ProcessBatch_FailureSchedulesBackoff verifies that on a
// publish failure the relay records the failure (single CTE UPDATE) and
// commits the same transaction, with retry hook fired and dead-letter hook
// only when the next retry crosses maxRetries (review C4).
func TestRelayService_ProcessBatch_FailureSchedulesBackoff(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	eventID := uuid.New()
	jobID := uuid.New()
	rows := sqlmock.NewRows([]string{
		"event_id", "event_type", "aggregate_type", "aggregate_id", "job_id",
		"attempt_id", "lease_id", "sequence", "emitted_at", "producer",
		"schema_version", "payload", "retry_count",
	}).AddRow(
		eventID.String(), "tasks.assigned", "task", uuid.New().String(), jobID.String(),
		nil, nil, int64(1), time.Now(), "manager",
		1, []byte(`{}`), 0,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(`FROM EVENT_OUTBOX`).
		WithArgs("events.deadletter", 100).
		WillReturnRows(rows)
	mock.ExpectExec(`UPDATE EVENT_OUTBOX o SET`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	var retryCalls, dlCalls int
	hook := &MetricsHook{
		IncRetry:      func(string) { retryCalls++ },
		IncDeadLetter: func(string) { dlCalls++ },
	}
	failingPub := &mockPublisher{failOn: map[uuid.UUID]bool{eventID: true}}

	svc := NewRelayService(store.NewOutboxStore(db), failingPub, RelayConfig{
		Enabled:    true,
		MaxRetries: 3,
		BatchSize:  100,
		Metrics:    hook,
	})

	if err := svc.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if retryCalls != 1 {
		t.Errorf("IncRetry calls = %d, want 1", retryCalls)
	}
	// retry_count starts at 0; +1 = 1, which is < maxRetries=3, so no DLQ yet.
	if dlCalls != 0 {
		t.Errorf("IncDeadLetter calls = %d, want 0 (not yet at maxRetries)", dlCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestNATSPublisher(t *testing.T) {
	// Try to connect to a local NATS instance; skip if unavailable
	pub, err := NewNATSPublisher("nats://localhost:4222", "")
	if err != nil {
		t.Skipf("NATS not available, skipping test: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(context.Background(), &events.EventEnvelope{EventID: uuid.New()}); err != nil {
		t.Errorf("NATSPublisher.Publish() should not error: %v", err)
	}
}

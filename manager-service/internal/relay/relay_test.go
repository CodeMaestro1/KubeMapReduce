package relay

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"kubemapreduce/manager-service/internal/events"
	"kubemapreduce/manager-service/internal/store"
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

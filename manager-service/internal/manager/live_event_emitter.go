package manager

import (
	"context"
	"database/sql"

	"kubemapreduce/manager-service/internal/events"
	"kubemapreduce/manager-service/internal/store"
	"kubemapreduce/manager-service/pkg/observability"
)

// LiveEventEmitter implements EventEmitter by inserting events into the
// EVENT_OUTBOX table for asynchronous relay to the message broker.
type LiveEventEmitter struct {
	outboxStore *store.OutboxStore
}

// NewLiveEventEmitter creates a LiveEventEmitter backed by the given outbox store.
func NewLiveEventEmitter(outboxStore *store.OutboxStore) *LiveEventEmitter {
	return &LiveEventEmitter{outboxStore: outboxStore}
}

// Emit inserts the event into the outbox table within the given transaction
// (transactional outbox pattern) or in a standalone transaction when tx is nil.
// On a successful insert it increments the events_emitted Prometheus counter so
// the metric reflects events that actually entered the outbox (not failed
// inserts). Errors are returned for logging only — the caller must never fail
// the orchestration path because of an emission failure.
func (l *LiveEventEmitter) Emit(ctx context.Context, tx *sql.Tx, event *events.EventEnvelope) error {
	if err := l.outboxStore.InsertEvent(ctx, tx, event); err != nil {
		return err
	}
	if m := observability.DefaultMetrics(); m != nil {
		m.EventsEmitted.WithLabelValues(string(event.EventType)).Inc()
	}
	return nil
}

// Close is a no-op for the outbox-backed emitter.
func (l *LiveEventEmitter) Close() error { return nil }

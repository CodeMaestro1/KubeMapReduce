package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"kubemapreduce/manager-service/internal/events"
)

// OutboxStore handles database operations for the EVENT_OUTBOX table.
// It provides methods to insert, mark delivered, and fetch undelivered events
// as part of the transactional outbox pattern.
type OutboxStore struct {
	db *sql.DB
}

// NewOutboxStore creates a new OutboxStore with the given database connection.
func NewOutboxStore(db *sql.DB) *OutboxStore {
	return &OutboxStore{db: db}
}

// InsertEvent inserts a new event into the outbox table.
// This should be called within the same transaction as the domain state change
// to ensure atomicity.
func (s *OutboxStore) InsertEvent(ctx context.Context, tx *sql.Tx, event *events.EventEnvelope) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	query := `
		INSERT INTO EVENT_OUTBOX (event_id, event_type, aggregate_type, aggregate_id, job_id, attempt_id, lease_id, sequence, emitted_at, producer, schema_version, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	args := []interface{}{
		event.EventID,
		string(event.EventType),
		string(event.AggregateType),
		event.AggregateID,
		event.JobID,
		event.AttemptID,
		event.LeaseID,
		event.Sequence,
		event.EmittedAt,
		event.Producer,
		event.SchemaVersion,
		payloadJSON,
	}

	if tx != nil {
		_, err = tx.ExecContext(ctx, query, args...)
	} else {
		_, err = s.db.ExecContext(ctx, query, args...)
	}

	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	return nil
}

// FetchUndeliveredEvents retrieves events that have not yet been delivered to the broker.
// Results are ordered by aggregate_type, aggregate_id, sequence to preserve ordering.
// limit controls the maximum number of events to return (0 means no limit).
func (s *OutboxStore) FetchUndeliveredEvents(ctx context.Context, limit int) ([]*events.EventEnvelope, error) {
	query := `
		SELECT event_id, event_type, aggregate_type, aggregate_id, job_id, attempt_id, lease_id, sequence, emitted_at, producer, schema_version, payload, retry_count, last_error
		FROM EVENT_OUTBOX
		WHERE delivered = FALSE AND event_type != $1
		ORDER BY aggregate_type, aggregate_id, sequence ASC`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, query, string(events.EventDeadLetter))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch undelivered events: %w", err)
	}
	defer rows.Close()

	var outboxEvents []*events.EventEnvelope
	for rows.Next() {
		var e events.EventEnvelope
		var payloadJSON []byte
		var attemptID, leaseID sql.NullString
		var retryCount int
		var lastError sql.NullString

		err := rows.Scan(
			&e.EventID,
			&e.EventType,
			&e.AggregateType,
			&e.AggregateID,
			&e.JobID,
			&attemptID,
			&leaseID,
			&e.Sequence,
			&e.EmittedAt,
			&e.Producer,
			&e.SchemaVersion,
			&payloadJSON,
			&retryCount,
			&lastError,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}

		if attemptID.Valid {
			id, _ := uuid.Parse(attemptID.String)
			e.AttemptID = &id
		}
		if leaseID.Valid {
			id, _ := uuid.Parse(leaseID.String)
			e.LeaseID = &id
		}

		if err := json.Unmarshal(payloadJSON, &e.Payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload for event %s: %w", e.EventID, err)
		}

		outboxEvents = append(outboxEvents, &e)
	}

	return outboxEvents, rows.Err()
}

// MarkDelivered marks an event as successfully delivered to the broker.
func (s *OutboxStore) MarkDelivered(ctx context.Context, eventID uuid.UUID) error {
	query := `UPDATE EVENT_OUTBOX SET delivered = TRUE, delivered_at = NOW() WHERE event_id = $1`
	_, err := s.db.ExecContext(ctx, query, eventID)
	if err != nil {
		return fmt.Errorf("failed to mark event %s as delivered: %w", eventID, err)
	}
	return nil
}

// RecordDeliveryFailure increments the retry count and records the error message.
// If maxRetries is exceeded, the event is moved to the dead letter queue
// by updating the event_type to events.EventDeadLetter.
func (s *OutboxStore) RecordDeliveryFailure(ctx context.Context, eventID uuid.UUID, errMsg string, maxRetries int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var retryCount int
	err = tx.QueryRowContext(ctx, `SELECT retry_count FROM EVENT_OUTBOX WHERE event_id = $1 FOR UPDATE`, eventID).Scan(&retryCount)
	if err != nil {
		return fmt.Errorf("failed to get retry count: %w", err)
	}

	retryCount++
	if retryCount >= maxRetries {
		_, err = tx.ExecContext(ctx,
			`UPDATE EVENT_OUTBOX SET event_type = $1, last_error = $2, retry_count = $3 WHERE event_id = $4`,
			string(events.EventDeadLetter), errMsg, retryCount, eventID)
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE EVENT_OUTBOX SET last_error = $1, retry_count = $2 WHERE event_id = $3`,
			errMsg, retryCount, eventID)
	}
	if err != nil {
		return fmt.Errorf("failed to record delivery failure: %w", err)
	}

	return tx.Commit()
}

// GetOutboxStats returns statistics about the outbox table for observability.
func (s *OutboxStore) GetOutboxStats(ctx context.Context) (undelivered int, delivered int, deadLettered int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM EVENT_OUTBOX WHERE delivered = FALSE AND event_type != $1`, string(events.EventDeadLetter)).Scan(&undelivered)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count undelivered: %w", err)
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM EVENT_OUTBOX WHERE delivered = TRUE`).Scan(&delivered)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count delivered: %w", err)
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM EVENT_OUTBOX WHERE event_type = $1`, string(events.EventDeadLetter)).Scan(&deadLettered)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count dead-lettered: %w", err)
	}
	return
}

// ReplayDeadLetteredEvents retrieves events from the DLQ for manual replay.
func (s *OutboxStore) ReplayDeadLetteredEvents(ctx context.Context, limit int) ([]*events.EventEnvelope, error) {
	query := `
		SELECT event_id, event_type, aggregate_type, aggregate_id, job_id, attempt_id, lease_id, sequence, emitted_at, producer, schema_version, payload
		FROM EVENT_OUTBOX
		WHERE event_type = $1
		ORDER BY retry_count DESC, emitted_at ASC`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, query, string(events.EventDeadLetter))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch dead-lettered events: %w", err)
	}
	defer rows.Close()

	var replayEvents []*events.EventEnvelope
	for rows.Next() {
		var e events.EventEnvelope
		var payloadJSON []byte
		var attemptID, leaseID sql.NullString

		err := rows.Scan(
			&e.EventID, &e.EventType, &e.AggregateType, &e.AggregateID, &e.JobID,
			&attemptID, &leaseID, &e.Sequence, &e.EmittedAt, &e.Producer, &e.SchemaVersion, &payloadJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dead-lettered event: %w", err)
		}

		if attemptID.Valid {
			id, _ := uuid.Parse(attemptID.String)
			e.AttemptID = &id
		}
		if leaseID.Valid {
			id, _ := uuid.Parse(leaseID.String)
			e.LeaseID = &id
		}

		if err := json.Unmarshal(payloadJSON, &e.Payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload for event %s: %w", e.EventID, err)
		}

		replayEvents = append(replayEvents, &e)
	}

	return replayEvents, rows.Err()
}

// ReprocessEvent resets a dead-lettered event for retry (moves back to pending queue).
func (s *OutboxStore) ReprocessEvent(ctx context.Context, eventID uuid.UUID) error {
	query := `UPDATE EVENT_OUTBOX SET event_type = $1, retry_count = 0, last_error = NULL WHERE event_id = $2 AND event_type = $3`
	_, err := s.db.ExecContext(ctx, query, string(events.TaskAttemptFailed), eventID, string(events.EventDeadLetter))
	if err != nil {
		return fmt.Errorf("failed to reprocess event %s: %w", eventID, err)
	}
	return nil
}

// UpdateSchemaVersion attempts to upconvert event payload schema.
// Returns the event with upconverted payload if migration was needed.
func (s *OutboxStore) UpconvertEventSchema(ctx context.Context, event *events.EventEnvelope, targetVersion int) (*events.EventEnvelope, error) {
	if event.SchemaVersion >= targetVersion {
		return event, nil
	}

	switch event.EventType {
	case events.TaskAssigned:
		// Re-marshal payload to get bytes for unmarshaling
		payloadBytes, err := json.Marshal(event.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal payload: %w", err)
		}
		var payload events.TaskAssignedPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
		// Example migration: add new field with default value
		if payload.WorkerID == "" {
			payload.WorkerID = "unknown-migrated"
		}
		event.Payload = payload
		event.SchemaVersion = targetVersion
	default:
		event.SchemaVersion = targetVersion
	}

	return event, nil
}

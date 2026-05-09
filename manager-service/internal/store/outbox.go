package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kubemapreduce/manager-service/internal/events"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// OutboxStore handles database operations for the EVENT_OUTBOX table.
//
// It implements the transactional-outbox pattern: producers insert events
// inside their domain transaction via [InsertEvent]; a relay claims rows
// via [ClaimUndeliveredEvents] (FOR UPDATE SKIP LOCKED) so that concurrent
// Manager replicas do not double-publish; success and failure are reported
// in batch via [MarkBatchDelivered] and [RecordDeliveryFailures].
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

// ClaimedEvent is an event returned by [ClaimUndeliveredEvents]. It carries
// the row's current retry_count so the relay can derive observability
// signals without an extra round-trip. Payload is delivered as
// json.RawMessage to avoid a lossy interface{} -> map[string]interface{}
// decode; consumers json.Unmarshal it into the typed payload they expect.
type ClaimedEvent struct {
	Envelope   *events.EventEnvelope
	RetryCount int
}

// ClaimUndeliveredEvents atomically locks and returns up to `limit`
// undelivered events whose next_attempt_at has elapsed. The query uses
// FOR UPDATE SKIP LOCKED so concurrent relay replicas do not race on the
// same row.
//
// The caller MUST commit (or rollback) the returned transaction; locked
// rows are released only when the transaction ends. Typical use:
// claim -> publish each event -> [MarkBatchDelivered] /
// [RecordDeliveryFailures] -> tx.Commit().
func (s *OutboxStore) ClaimUndeliveredEvents(ctx context.Context, limit int) (*sql.Tx, []ClaimedEvent, error) {
	if limit <= 0 {
		return nil, nil, errors.New("limit must be positive")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin claim tx: %w", err)
	}

	const query = `
		SELECT event_id, event_type, aggregate_type, aggregate_id, job_id, attempt_id, lease_id, sequence, emitted_at, producer, schema_version, payload, retry_count
		FROM EVENT_OUTBOX
		WHERE delivered = FALSE
		  AND event_type <> $1
		  AND next_attempt_at <= NOW()
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT $2`

	rows, err := tx.QueryContext(ctx, query, string(events.EventDeadLetter), limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, fmt.Errorf("failed to claim undelivered events: %w", err)
	}

	claims, scanErr := scanClaimedEvents(rows)
	rows.Close()
	if scanErr != nil {
		_ = tx.Rollback()
		return nil, nil, scanErr
	}
	return tx, claims, nil
}

func scanClaimedEvents(rows *sql.Rows) ([]ClaimedEvent, error) {
	var out []ClaimedEvent
	for rows.Next() {
		var e events.EventEnvelope
		var payloadJSON []byte
		var attemptID, leaseID sql.NullString
		var retryCount int

		if err := rows.Scan(
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
		); err != nil {
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
		e.Payload = json.RawMessage(payloadJSON)
		out = append(out, ClaimedEvent{Envelope: &e, RetryCount: retryCount})
	}
	return out, rows.Err()
}

// MarkBatchDelivered marks the given events delivered in a single UPDATE
// against the supplied transaction. The caller is responsible for commit.
func (s *OutboxStore) MarkBatchDelivered(ctx context.Context, tx *sql.Tx, eventIDs []uuid.UUID) error {
	if len(eventIDs) == 0 {
		return nil
	}
	ids := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		ids[i] = id.String()
	}
	const query = `UPDATE EVENT_OUTBOX SET delivered = TRUE, delivered_at = NOW() WHERE event_id = ANY($1::uuid[])`
	if _, err := tx.ExecContext(ctx, query, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed to mark events delivered: %w", err)
	}
	return nil
}

// FailureRecord describes one delivery failure for [RecordDeliveryFailures].
type FailureRecord struct {
	EventID uuid.UUID
	Err     string
}

// RecordDeliveryFailures bumps retry_count, stores last_error, and
// schedules the next attempt with exponential backoff (via the
// outbox_backoff SQL helper). Rows that exhaust maxRetries are dead-
// lettered while their original event_type is preserved in
// original_event_type for replay safety (review C4). All updates run in
// one statement against the supplied transaction.
func (s *OutboxStore) RecordDeliveryFailures(
	ctx context.Context,
	tx *sql.Tx,
	failures []FailureRecord,
	maxRetries int,
) error {
	if len(failures) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(failures))
	args := make([]interface{}, 0, len(failures)*2+1)
	args = append(args, string(events.EventDeadLetter))
	for i, f := range failures {
		base := i*2 + 2
		placeholders = append(placeholders, fmt.Sprintf("($%d::uuid, $%d::text)", base, base+1))
		args = append(args, f.EventID, f.Err)
	}

	query := fmt.Sprintf(`
		WITH input(event_id, err) AS (
			VALUES %s
		)
		UPDATE EVENT_OUTBOX o SET
			retry_count = o.retry_count + 1,
			last_error  = input.err,
			next_attempt_at = NOW() + outbox_backoff(o.retry_count + 1),
			original_event_type = CASE
				WHEN o.retry_count + 1 >= %d AND o.event_type <> $1 THEN o.event_type
				ELSE o.original_event_type
			END,
			event_type = CASE
				WHEN o.retry_count + 1 >= %d THEN $1
				ELSE o.event_type
			END
		FROM input
		WHERE o.event_id = input.event_id`,
		strings.Join(placeholders, ","), maxRetries, maxRetries)

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to record delivery failures: %w", err)
	}
	return nil
}

// EnsureBackoffFunction installs the outbox_backoff(retries int) helper
// used by [RecordDeliveryFailures]. Idempotent; call once at service
// startup. Kept as a Go-driven CREATE OR REPLACE so the algorithm can
// evolve without a schema change.
func (s *OutboxStore) EnsureBackoffFunction(ctx context.Context) error {
	const ddl = `
		CREATE OR REPLACE FUNCTION outbox_backoff(retries INT) RETURNS INTERVAL
		LANGUAGE plpgsql AS $$
		DECLARE
			base_ms NUMERIC := 1000;
			max_ms  NUMERIC := 30000;
			expo_ms NUMERIC;
		BEGIN
			IF retries < 1 THEN retries := 1; END IF;
			expo_ms := LEAST(base_ms * power(2, retries - 1), max_ms);
			RETURN make_interval(secs => (expo_ms + random() * (expo_ms / 4)) / 1000.0);
		END;$$;`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to install outbox_backoff function: %w", err)
	}
	return nil
}

// PurgeDeliveredOlderThan deletes successfully-delivered rows older than
// the cutoff. Run periodically to bound table growth; the relay's claim
// query is unaffected because it filters delivered = FALSE.
func (s *OutboxStore) PurgeDeliveredOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM EVENT_OUTBOX WHERE delivered = TRUE AND delivered_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to purge delivered events: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
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

// ReplayDeadLetteredEvents retrieves events from the DLQ for manual
// inspection. Payload is returned as json.RawMessage; see
// [ClaimUndeliveredEvents] for rationale.
func (s *OutboxStore) ReplayDeadLetteredEvents(ctx context.Context, limit int) ([]*events.EventEnvelope, error) {
	const baseQuery = `
		SELECT event_id, event_type, aggregate_type, aggregate_id, job_id, attempt_id, lease_id, sequence, emitted_at, producer, schema_version, payload
		FROM EVENT_OUTBOX
		WHERE event_type = $1
		ORDER BY retry_count DESC, emitted_at ASC`

	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = s.db.QueryContext(ctx, baseQuery+" LIMIT $2", string(events.EventDeadLetter), limit)
	} else {
		rows, err = s.db.QueryContext(ctx, baseQuery, string(events.EventDeadLetter))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch dead-lettered events: %w", err)
	}
	defer rows.Close()

	var replayEvents []*events.EventEnvelope
	for rows.Next() {
		var e events.EventEnvelope
		var payloadJSON []byte
		var attemptID, leaseID sql.NullString

		if err := rows.Scan(
			&e.EventID, &e.EventType, &e.AggregateType, &e.AggregateID, &e.JobID,
			&attemptID, &leaseID, &e.Sequence, &e.EmittedAt, &e.Producer, &e.SchemaVersion, &payloadJSON,
		); err != nil {
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
		e.Payload = json.RawMessage(payloadJSON)
		replayEvents = append(replayEvents, &e)
	}

	return replayEvents, rows.Err()
}

// ReprocessEvent resets a dead-lettered event for retry, restoring its
// original event_type so downstream consumers see the correct semantics
// after replay (review C4). Returns sql.ErrNoRows when the event is not
// currently in the DLQ.
func (s *OutboxStore) ReprocessEvent(ctx context.Context, eventID uuid.UUID) error {
	const query = `
		UPDATE EVENT_OUTBOX SET
			event_type = COALESCE(original_event_type, event_type),
			original_event_type = NULL,
			retry_count = 0,
			last_error = NULL,
			next_attempt_at = NOW()
		WHERE event_id = $1 AND event_type = $2`

	res, err := s.db.ExecContext(ctx, query, eventID, string(events.EventDeadLetter))
	if err != nil {
		return fmt.Errorf("failed to reprocess event %s: %w", eventID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// ErrUnknownEventTypeMigration is returned by [UpconvertEventSchema] when
// the requested target version exceeds what this binary knows how to
// migrate for the given event_type. Returning an error rather than
// silently bumping schema_version surfaces missed producer-side updates
// (review M3).
var ErrUnknownEventTypeMigration = errors.New("no migration registered for event_type at requested target version")

// UpconvertEventSchema upgrades a payload's schema_version when needed.
// It refuses to bump the version for event types it does not know how to
// migrate; callers should treat this as a hard error in tests and a
// metric-emitting warning in production rather than ignoring it.
func (s *OutboxStore) UpconvertEventSchema(ctx context.Context, event *events.EventEnvelope, targetVersion int) (*events.EventEnvelope, error) {
	if event.SchemaVersion >= targetVersion {
		return event, nil
	}

	switch event.EventType {
	case events.TaskAssigned:
		payloadBytes, err := json.Marshal(event.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal payload: %w", err)
		}
		var payload events.TaskAssignedPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
		if payload.WorkerID == "" {
			payload.WorkerID = "unknown-migrated"
		}
		event.Payload = payload
		event.SchemaVersion = targetVersion
		return event, nil
	default:
		return nil, fmt.Errorf("%w: type=%s current=%d target=%d",
			ErrUnknownEventTypeMigration, event.EventType, event.SchemaVersion, targetVersion)
	}
}

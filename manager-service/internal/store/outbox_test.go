package store

import (
	"context"
	"testing"
	"time"

	"kubemapreduce/manager-service/internal/events"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestInsertEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)

	eventID := uuid.New()
	jobID := uuid.New()
	taskID := uuid.New()

	event := &events.EventEnvelope{
		EventID:       eventID,
		EventType:     events.TaskAssigned,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		Sequence:      1,
		EmittedAt:     time.Now().UTC(),
		Producer:      "test",
		SchemaVersion: 1,
		Payload:       events.TaskAssignedPayload{TaskType: "Map", WorkerID: "w1", LeaseTTL: 30},
	}

	mock.ExpectExec(`INSERT INTO EVENT_OUTBOX`).
		WithArgs(
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
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.InsertEvent(context.Background(), nil, event); err != nil {
		t.Fatalf("InsertEvent() returned error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestInsertEvent_Tx(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)

	eventID := uuid.New()
	jobID := uuid.New()
	taskID := uuid.New()

	event := &events.EventEnvelope{
		EventID:       eventID,
		EventType:     events.TaskAttemptCompleted,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		Sequence:      1,
		EmittedAt:     time.Now().UTC(),
		Producer:      "test",
		SchemaVersion: 1,
		Payload:       events.TaskAttemptCompletedPayload{WorkerID: "w1"},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO EVENT_OUTBOX`).
		WithArgs(
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
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}

	if err := s.InsertEvent(context.Background(), tx, event); err != nil {
		t.Fatalf("InsertEvent() with tx returned error: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestMarkBatchDelivered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	id1, id2 := uuid.New(), uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET delivered = TRUE, delivered_at = NOW\(\) WHERE event_id = ANY\(\$1::uuid\[\]\)`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.MarkBatchDelivered(context.Background(), tx, []uuid.UUID{id1, id2}); err != nil {
		t.Fatalf("MarkBatchDelivered: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestMarkBatchDelivered_Empty(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	// Should be a no-op without touching the DB or tx.
	if err := s.MarkBatchDelivered(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRecordDeliveryFailures_NoOpOnEmpty(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	if err := s.RecordDeliveryFailures(context.Background(), nil, nil, 3); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRecordDeliveryFailures_BatchedSingleStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	id1, id2 := uuid.New(), uuid.New()

	mock.ExpectBegin()
	// One UPDATE for both rows; we don't pin the exact SQL so the
	// helper retains room to evolve, but we do assert it is a single
	// statement, not one per row.
	mock.ExpectExec(`UPDATE EVENT_OUTBOX o SET`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	failures := []FailureRecord{{EventID: id1, Err: "boom"}, {EventID: id2, Err: "boom"}}
	if err := s.RecordDeliveryFailures(context.Background(), tx, failures, 3); err != nil {
		t.Fatalf("RecordDeliveryFailures: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestClaimUndeliveredEvents_RejectsZeroLimit(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	if _, _, err := s.ClaimUndeliveredEvents(context.Background(), 0); err == nil {
		t.Fatal("expected error for non-positive limit")
	}
}

func TestReprocessEvent_RestoresOriginalEventType(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	eventID := uuid.New()

	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET\s+event_type = COALESCE\(original_event_type, event_type\)`).
		WithArgs(eventID, string(events.EventDeadLetter)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.ReprocessEvent(context.Background(), eventID); err != nil {
		t.Fatalf("ReprocessEvent: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestReprocessEvent_NotInDLQ(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	eventID := uuid.New()

	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET`).
		WithArgs(eventID, string(events.EventDeadLetter)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = s.ReprocessEvent(context.Background(), eventID)
	if err == nil {
		t.Fatal("expected error when event not in DLQ")
	}
}

func TestUpconvertEventSchema_UnknownTypeReturnsError(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	evt := &events.EventEnvelope{
		EventType:     events.JobSubmitted,
		SchemaVersion: 1,
	}
	_, err = s.UpconvertEventSchema(context.Background(), evt, 2)
	if err == nil {
		t.Fatal("expected ErrUnknownEventTypeMigration for unhandled event type")
	}
}

func TestGetOutboxStats(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM EVENT_OUTBOX WHERE delivered = FALSE AND event_type != \$1`).
		WithArgs(string(events.EventDeadLetter)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM EVENT_OUTBOX WHERE delivered = TRUE`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM EVENT_OUTBOX WHERE event_type = \$1`).
		WithArgs(string(events.EventDeadLetter)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	undelivered, delivered, deadLettered, err := s.GetOutboxStats(context.Background())
	if err != nil {
		t.Fatalf("GetOutboxStats() returned error: %v", err)
	}

	if undelivered != 3 {
		t.Errorf("undelivered = %d, want 3", undelivered)
	}
	if delivered != 10 {
		t.Errorf("delivered = %d, want 10", delivered)
	}
	if deadLettered != 1 {
		t.Errorf("deadLettered = %d, want 1", deadLettered)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

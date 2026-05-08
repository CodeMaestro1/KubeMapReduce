package store

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"kubemapreduce/manager-service/internal/events"
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

func TestMarkDelivered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	eventID := uuid.New()

	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET delivered = TRUE, delivered_at = NOW\(\) WHERE event_id = \$1`).
		WithArgs(eventID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkDelivered(context.Background(), eventID); err != nil {
		t.Fatalf("MarkDelivered() returned error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestRecordDeliveryFailure_MaxRetries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	eventID := uuid.New()

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"retry_count"}).AddRow(2)
	mock.ExpectQuery(`SELECT retry_count FROM EVENT_OUTBOX WHERE event_id = \$1 FOR UPDATE`).
		WithArgs(eventID).
		WillReturnRows(rows)
	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET event_type = \$1, last_error = \$2, retry_count = \$3 WHERE event_id = \$4`).
		WithArgs(string(events.EventDeadLetter), "broker unreachable", 3, eventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := s.RecordDeliveryFailure(context.Background(), eventID, "broker unreachable", 3); err != nil {
		t.Fatalf("RecordDeliveryFailure() returned error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestRecordDeliveryFailure_Retry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	s := NewOutboxStore(db)
	eventID := uuid.New()

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"retry_count"}).AddRow(0)
	mock.ExpectQuery(`SELECT retry_count FROM EVENT_OUTBOX WHERE event_id = \$1 FOR UPDATE`).
		WithArgs(eventID).
		WillReturnRows(rows)
	mock.ExpectExec(`UPDATE EVENT_OUTBOX SET last_error = \$1, retry_count = \$2 WHERE event_id = \$3`).
		WithArgs("timeout", 1, eventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := s.RecordDeliveryFailure(context.Background(), eventID, "timeout", 3); err != nil {
		t.Fatalf("RecordDeliveryFailure() returned error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
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

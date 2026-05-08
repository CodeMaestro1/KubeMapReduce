package manager

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"kubemapreduce/manager-service/internal/events"
)

// EventEmitter is the interface for publishing lifecycle events during state
// transitions. Implementations may write to an outbox table, publish directly
// to a broker, or be a no-op when the feature is disabled.
//
// When tx is non-nil the event is inserted within the same database transaction
// as the domain state change (transactional outbox pattern). When tx is nil the
// event is persisted in a standalone transaction (shadow publish fallback).
// Errors are logged by callers but must never fail the orchestration path.
type EventEmitter interface {
	Emit(ctx context.Context, tx *sql.Tx, event *events.EventEnvelope) error
	Close() error
}

// NoopEventEmitter is a no-op implementation used when the outbox relay is disabled.
type NoopEventEmitter struct{}

func (n *NoopEventEmitter) Emit(ctx context.Context, tx *sql.Tx, event *events.EventEnvelope) error {
	return nil
}
func (n *NoopEventEmitter) Close() error { return nil }

// eventBuilder provides convenient factory methods for constructing event envelopes.
type eventBuilder struct {
	producer string
}

func newEventBuilder(producer string) *eventBuilder {
	return &eventBuilder{producer: producer}
}

func (b *eventBuilder) jobSubmitted(jobID uuid.UUID, userID uuid.UUID, inputURI string, mTasks, rTasks int) *events.EventEnvelope {
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.JobSubmitted,
		AggregateType: events.AggregateTypeJob,
		AggregateID:   jobID,
		JobID:         jobID,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.JobSubmittedPayload{
			UserID:   userID,
			InputURI: inputURI,
			MTasks:   mTasks,
			RTasks:   rTasks,
		},
	}
}

func (b *eventBuilder) jobStateChanged(jobID uuid.UUID, oldStatus, newStatus string) *events.EventEnvelope {
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.JobStateChanged,
		AggregateType: events.AggregateTypeJob,
		AggregateID:   jobID,
		JobID:         jobID,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.JobStateChangedPayload{
			OldStatus: oldStatus,
			NewStatus: newStatus,
		},
	}
}

func (b *eventBuilder) jobCancelRequested(jobID uuid.UUID) *events.EventEnvelope {
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.JobCancelRequested,
		AggregateType: events.AggregateTypeJob,
		AggregateID:   jobID,
		JobID:         jobID,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload:       struct{}{},
	}
}

func (b *eventBuilder) taskAssigned(taskID uuid.UUID, jobID uuid.UUID, taskType string, workerID string, leaseTTL int) *events.EventEnvelope {
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.TaskAssigned,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.TaskAssignedPayload{
			TaskType: taskType,
			WorkerID: workerID,
			LeaseTTL: leaseTTL,
		},
	}
}

func (b *eventBuilder) heartbeatReceived(taskID uuid.UUID, jobID uuid.UUID, attemptID uuid.UUID) *events.EventEnvelope {
	aid := attemptID
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.TaskHeartbeatReceived,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		AttemptID:     &aid,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload:       struct{}{},
	}
}

func (b *eventBuilder) attemptCompleted(taskID uuid.UUID, jobID uuid.UUID, attemptID uuid.UUID, workerID string) *events.EventEnvelope {
	aid := attemptID
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.TaskAttemptCompleted,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		AttemptID:     &aid,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.TaskAttemptCompletedPayload{
			WorkerID: workerID,
		},
	}
}

func (b *eventBuilder) attemptFailed(taskID uuid.UUID, jobID uuid.UUID, attemptID uuid.UUID, workerID string, reason string) *events.EventEnvelope {
	aid := attemptID
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.TaskAttemptFailed,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		AttemptID:     &aid,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.TaskAttemptFailedPayload{
			WorkerID: workerID,
			Reason:   reason,
		},
	}
}

func (b *eventBuilder) taskReaped(taskID uuid.UUID, jobID uuid.UUID, attemptID uuid.UUID, reason string) *events.EventEnvelope {
	aid := attemptID
	return &events.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     events.TaskReaped,
		AggregateType: events.AggregateTypeTask,
		AggregateID:   taskID,
		JobID:         jobID,
		AttemptID:     &aid,
		Sequence:      0,
		EmittedAt:     time.Now().UTC(),
		Producer:      b.producer,
		SchemaVersion: events.SchemaVersion,
		Payload: events.TaskReapedPayload{
			Reason: reason,
		},
	}
}

package events

import (
	"time"

	"github.com/google/uuid"
)

// SchemaVersion is the current version of the event envelope.
// Versioning policy:
//   - Major version bumps (1 -> 2) indicate breaking changes (removed fields, changed semantics).
//   - Minor version bumps (1.0 -> 1.1) indicate backward-compatible additions (new optional fields).
//   - Consumers must tolerate unknown fields (forward-compatible reads).
//   - Producers must not remove fields without a major version bump.
const SchemaVersion = 1

// EventType represents the type of domain event being emitted.
type EventType string

const (
	// Job lifecycle events
	JobSubmitted       EventType = "jobs.submitted"
	JobStateChanged    EventType = "jobs.state.changed"
	JobCancelRequested EventType = "jobs.cancel.requested"

	// Task lifecycle events
	TaskIdleDetected      EventType = "tasks.idle.detected"
	TaskAssigned          EventType = "tasks.assigned"
	TaskHeartbeatReceived EventType = "tasks.heartbeat.received"
	TaskAttemptCompleted  EventType = "tasks.attempt.completed"
	TaskAttemptFailed     EventType = "tasks.attempt.failed"
	TaskReaped            EventType = "tasks.reaped"

	// Control/configuration events
	SystemConfigUpdated EventType = "system.config.updated"
	SystemReconcileTick EventType = "system.reconcile.tick"

	// Audit/DLQ events
	EventDeadLetter      EventType = "events.deadletter"
	EventReplayRequested EventType = "events.replay.requested"
)

// AggregateType represents the aggregate root the event belongs to.
type AggregateType string

const (
	AggregateTypeJob    AggregateType = "job"
	AggregateTypeTask   AggregateType = "task"
	AggregateTypeSystem AggregateType = "system"
)

// EventEnvelope is the canonical wrapper for all events emitted by the system.
// It provides a consistent structure for routing, ordering, and replay.
type EventEnvelope struct {
	EventID       uuid.UUID     `json:"event_id" db:"event_id"`
	EventType     EventType     `json:"event_type" db:"event_type"`
	AggregateType AggregateType `json:"aggregate_type" db:"aggregate_type"`
	AggregateID   uuid.UUID     `json:"aggregate_id" db:"aggregate_id"`
	JobID         uuid.UUID     `json:"job_id" db:"job_id"`
	AttemptID     *uuid.UUID    `json:"attempt_id,omitempty" db:"attempt_id"`
	LeaseID       *uuid.UUID    `json:"lease_id,omitempty" db:"lease_id"`
	Sequence      int64         `json:"sequence" db:"sequence"`
	EmittedAt     time.Time     `json:"emitted_at" db:"emitted_at"`
	Producer      string        `json:"producer" db:"producer"`
	SchemaVersion int           `json:"schema_version" db:"schema_version"`
	Payload       interface{}   `json:"payload" db:"payload"`
}

// SequenceGenerator provides monotonically increasing sequence numbers
// per aggregate (job_id or task_id) for ordering guarantees.
type SequenceGenerator interface {
	NextSequence(aggregateID uuid.UUID) (int64, error)
}

// JobSubmittedPayload contains the data for a job.submitted event.
type JobSubmittedPayload struct {
	UserID      uuid.UUID `json:"user_id"`
	InputURI    string    `json:"input_uri"`
	MTasks      int       `json:"m_tasks"`
	RTasks      int       `json:"r_tasks"`
	MapperURI   string    `json:"mapper_uri"`
	ReducerURI  string    `json:"reducer_uri"`
	CombinerURI string    `json:"combiner_uri,omitempty"`
}

// JobStateChangedPayload contains the data for a job.state.changed event.
type JobStateChangedPayload struct {
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
}

// TaskAssignedPayload contains the data for a tasks.assigned event.
type TaskAssignedPayload struct {
	TaskType  string   `json:"task_type"`
	WorkerID  string   `json:"worker_id"`
	LeaseTTL  int      `json:"lease_ttl"`
	InputURIs []string `json:"input_uris,omitempty"`
}

// TaskAttemptCompletedPayload contains data for a tasks.attempt.completed event.
type TaskAttemptCompletedPayload struct {
	WorkerID       string   `json:"worker_id"`
	OutputURIs     []string `json:"output_uris,omitempty"`
	PartitionIndex *int     `json:"partition_index,omitempty"`
}

// TaskAttemptFailedPayload contains data for a tasks.attempt.failed event.
type TaskAttemptFailedPayload struct {
	WorkerID string `json:"worker_id"`
	Reason   string `json:"reason,omitempty"`
}

// TaskReapedPayload contains data for a tasks.reaped event.
type TaskReapedPayload struct {
	WorkerID string `json:"worker_id"`
	Reason   string `json:"reason"`
}

package events

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventTypeConstants(t *testing.T) {
	tests := []struct {
		eventType EventType
		want      string
	}{
		{JobSubmitted, "jobs.submitted"},
		{JobStateChanged, "jobs.state.changed"},
		{JobCancelRequested, "jobs.cancel.requested"},
		{TaskIdleDetected, "tasks.idle.detected"},
		{TaskAssigned, "tasks.assigned"},
		{TaskHeartbeatReceived, "tasks.heartbeat.received"},
		{TaskAttemptCompleted, "tasks.attempt.completed"},
		{TaskAttemptFailed, "tasks.attempt.failed"},
		{TaskReaped, "tasks.reaped"},
		{SystemConfigUpdated, "system.config.updated"},
		{SystemReconcileTick, "system.reconcile.tick"},
		{EventDeadLetter, "events.deadletter"},
		{EventReplayRequested, "events.replay.requested"},
	}

	for _, tt := range tests {
		if string(tt.eventType) != tt.want {
			t.Errorf("EventType = %q, want %q", tt.eventType, tt.want)
		}
	}
}

func TestAggregateTypeConstants(t *testing.T) {
	tests := []struct {
		aggType AggregateType
		want    string
	}{
		{AggregateTypeJob, "job"},
		{AggregateTypeTask, "task"},
		{AggregateTypeSystem, "system"},
	}

	for _, tt := range tests {
		if string(tt.aggType) != tt.want {
			t.Errorf("AggregateType = %q, want %q", tt.aggType, tt.want)
		}
	}
}

func TestEventEnvelopeCreation(t *testing.T) {
	eventID := uuid.New()
	jobID := uuid.New()
	aggregateID := uuid.New()
	attemptID := uuid.New()
	leaseID := uuid.New()
	now := time.Now().UTC()

	env := EventEnvelope{
		EventID:       eventID,
		EventType:     TaskAssigned,
		AggregateType: AggregateTypeTask,
		AggregateID:   aggregateID,
		JobID:         jobID,
		AttemptID:     &attemptID,
		LeaseID:       &leaseID,
		Sequence:      1,
		EmittedAt:     now,
		Producer:      "manager-scheduler",
		SchemaVersion: 1,
		Payload:       TaskAssignedPayload{TaskType: "Map", WorkerID: "worker-1", LeaseTTL: 30},
	}

	if env.EventID != eventID {
		t.Errorf("EventID = %v, want %v", env.EventID, eventID)
	}
	if env.EventType != TaskAssigned {
		t.Errorf("EventType = %v, want %v", env.EventType, TaskAssigned)
	}
	if env.AggregateType != AggregateTypeTask {
		t.Errorf("AggregateType = %v, want %v", env.AggregateType, AggregateTypeTask)
	}
	if env.Sequence != 1 {
		t.Errorf("Sequence = %d, want 1", env.Sequence)
	}
	if env.Producer != "manager-scheduler" {
		t.Errorf("Producer = %q, want %q", env.Producer, "manager-scheduler")
	}
	if env.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", env.SchemaVersion, SchemaVersion)
	}
	if env.AttemptID == nil || *env.AttemptID != attemptID {
		t.Errorf("AttemptID mismatch")
	}
	if env.LeaseID == nil || *env.LeaseID != leaseID {
		t.Errorf("LeaseID mismatch")
	}
}

func TestSchemaVersionConstant(t *testing.T) {
	if SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", SchemaVersion)
	}
}

func TestPayloadStructs(t *testing.T) {
	userID := uuid.New()
	jobPayload := JobSubmittedPayload{
		UserID:   userID,
		InputURI: "s3://input/data.txt",
		MTasks:   4,
		RTasks:   2,
	}
	if jobPayload.UserID != userID {
		t.Errorf("JobSubmittedPayload.UserID mismatch")
	}
	if jobPayload.MTasks != 4 {
		t.Errorf("JobSubmittedPayload.MTasks = %d, want 4", jobPayload.MTasks)
	}

	taskAssigned := TaskAssignedPayload{
		TaskType: "Map",
		WorkerID: "worker-1",
		LeaseTTL: 30,
	}
	if taskAssigned.WorkerID != "worker-1" {
		t.Errorf("TaskAssignedPayload.WorkerID = %q, want %q", taskAssigned.WorkerID, "worker-1")
	}
	if taskAssigned.LeaseTTL != 30 {
		t.Errorf("TaskAssignedPayload.LeaseTTL = %d, want 30", taskAssigned.LeaseTTL)
	}

	completedPayload := TaskAttemptCompletedPayload{
		WorkerID: "worker-1",
	}
	if completedPayload.WorkerID != "worker-1" {
		t.Errorf("TaskAttemptCompletedPayload.WorkerID mismatch")
	}

	failedPayload := TaskAttemptFailedPayload{
		WorkerID: "worker-2",
		Reason:   "OOM",
	}
	if failedPayload.Reason != "OOM" {
		t.Errorf("TaskAttemptFailedPayload.Reason = %q, want %q", failedPayload.Reason, "OOM")
	}

	reapedPayload := TaskReapedPayload{
		WorkerID: "worker-3",
		Reason:   "lease expired",
	}
	if reapedPayload.Reason != "lease expired" {
		t.Errorf("TaskReapedPayload.Reason mismatch")
	}
}

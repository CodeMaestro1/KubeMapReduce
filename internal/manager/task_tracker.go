package manager

import "time"

// TaskState defines the current status of a Map or Reduce task
//
// State transitions:
//   - Idle -> InProgress (via GetNextTask)
//   - InProgress -> Completed (via CompleteTask)
//   - InProgress -> Idle (via FailTask for retries)
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// TaskType defines whether a task is a Map or Reduce task.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	default:
		return "Unknown"
	}
}

// Task represents the metadata the Master needs to track
type Task struct {
	ID              string
	Type            TaskType // MapTask or ReduceTask
	State           TaskState
	workerID        string
	InputFile       string
	startTime       time.Time
	ActiveAttemptID string
	LeaseID         string
	LastHeartbeat   time.Time
	OutputURIs      []string
	OutputChecksums []string
	Attempts        int
	LastFailure     string
	LastFailedAt    time.Time
}

// TaskTracker tracks the overall job progress.
// Note: Callers MUST NOT directly mutate the fields of this struct once
// it has been passed to NewScheduler, as the Scheduler maintains an internal
// index that will become out of sync.
type TaskTracker struct {
	mapTasks    []Task
	reduceTasks []Task
}

func (t *Task) WorkerID() string        { return t.workerID }
func (t *Task) StartTime() time.Time    { return t.startTime }
func (t *Task) GetAttemptID() string    { return t.ActiveAttemptID }
func (t *Task) GetLeaseID() string      { return t.LeaseID }
func (t *Task) GetHeartbeat() time.Time { return t.LastHeartbeat }

// NewTaskTracker safely initializes a TaskTracker by maintaining defensive copies.
func NewTaskTracker(mapTasks, reduceTasks []Task) *TaskTracker {
	mCopy := make([]Task, len(mapTasks))
	copy(mCopy, mapTasks)
	rCopy := make([]Task, len(reduceTasks))
	copy(rCopy, reduceTasks)
	return &TaskTracker{
		mapTasks:    mCopy,
		reduceTasks: rCopy,
	}
}

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
	// Note: failed tasks are transitioned back to Idle by the scheduler; no separate Failed state is used.
)

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
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
	ID        string
	Type      TaskType // MapTask or ReduceTask
	State     TaskState
	WorkerID  string
	InputFile string
	StartTime time.Time
}

// TaskTracker tracks the overall job progress.
// Note: Callers MUST NOT directly mutate the fields of this struct once
// it has been passed to NewScheduler, as the Scheduler maintains an internal
// index that will become out of sync.
type TaskTracker struct {
	MapTasks    []Task
	ReduceTasks []Task
}

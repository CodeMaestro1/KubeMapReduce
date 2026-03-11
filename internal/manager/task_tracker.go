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

// TaskType defines whether a task is a Map or Reduce task.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

// Task represents the metadata the Master needs to track
type Task struct {
	ID        string
	Type      TaskType // MapTask or ReduceTask
	State     TaskState
	WorkerID  string
	InputFile string
	StartTime time.Time
}

// TaskTracker tracks the overall job progress
type TaskTracker struct {
	MapTasks    []Task
	ReduceTasks []Task
}

package manager

import "time"

// TaskState defines the current status of a Map or Reduce task
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

// Task represents the metadata the Master needs to track
type Task struct {
	ID        string
	Type      string // "Map" or "Reduce"
	State     TaskState
	WorkerID  string
	InputFile string
	StartTime time.Time
}

// Manager tracks the overall job progress
type TaskTracker struct {
	MapTasks    []Task
	ReduceTasks []Task
}

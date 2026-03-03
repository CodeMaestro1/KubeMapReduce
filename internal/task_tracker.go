package manager

import "time"

// TaskState defines the current status of a Map or Reduce task [cite: 355]
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

// Task represents the metadata the Master needs to track [cite: 355]
type Task struct {
	ID        string
	Type      string // "Map" or "Reduce"
	State     TaskState
	WorkerID  string // Identity of the worker machine [cite: 355]
	InputFile string
	StartTime time.Time
}

// Manager tracks the overall job progress [cite: 355]
type TaskTracker struct {
	MapTasks    []Task
	ReduceTasks []Task
}

package manager

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNoIdleTasks            = errors.New("no idle tasks available right now, please wait")
	ErrJobCompleted           = errors.New("all tasks completed, job is done")
	ErrTaskNotFound           = errors.New("task not found")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrNilTracker             = errors.New("tracker cannot be nil")
	ErrDuplicateTaskID        = errors.New("duplicate task ID found")
	ErrInvalidInitialState    = errors.New("all initial tasks must be Idle")
	ErrEmptyWorkerID          = errors.New("workerID cannot be empty")
	ErrInvalidTaskType        = errors.New("task found in wrong collection type (map vs reduce)")
)

// Scheduler coordinates task assignment and lifecycle for a single MapReduce job.

type Scheduler struct {
	mu      sync.Mutex
	tracker *TaskTracker
	taskMap map[string]*Task // O(1) lookups by task ID
}

// NewScheduler initializes a Scheduler with O(1) task index mapping.
// It will validate that the tracker is not nil, all internal tasks are Idle,
// and that all task IDs are unique within the MapReduce job graph.
func NewScheduler(tracker *TaskTracker) (*Scheduler, error) {
	if tracker == nil {
		return nil, ErrNilTracker
	}

	taskMap := make(map[string]*Task)

	// Validate MapTasks
	for i := range tracker.MapTasks {
		task := &tracker.MapTasks[i]
		if task.State != Idle {
			return nil, ErrInvalidInitialState
		}
		if task.Type != MapTask {
			return nil, ErrInvalidTaskType
		}
		if _, exists := taskMap[task.ID]; exists {
			return nil, ErrDuplicateTaskID
		}
		taskMap[task.ID] = task
	}

	// Validate ReduceTasks
	for i := range tracker.ReduceTasks {
		task := &tracker.ReduceTasks[i]
		if task.State != Idle {
			return nil, ErrInvalidInitialState
		}
		if task.Type != ReduceTask {
			return nil, ErrInvalidTaskType
		}
		if _, exists := taskMap[task.ID]; exists {
			return nil, ErrDuplicateTaskID
		}
		taskMap[task.ID] = task
	}

	return &Scheduler{
		tracker: tracker,
		taskMap: taskMap,
	}, nil
}

// GetNextTask returns the next available idle task for the given worker.
//
// Map tasks are always scheduled first: while there are any map tasks that are
// not yet completed, this method will only return map tasks. Once all map
// tasks have been completed, it will start returning idle reduce tasks.
//
// On success it returns a pointer to the task that was moved to InProgress
// for the provided worker ID. If there are remaining (map or reduce) tasks
// but none are currently idle (i.e. all are InProgress), it returns
// ErrNoIdleTasks. If all map and reduce tasks have completed, it returns
// ErrJobCompleted.
func (s *Scheduler) GetNextTask(workerID string) (*Task, error) {
	if workerID == "" {
		return nil, ErrEmptyWorkerID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	allMapCompleted := true
	for i := range s.tracker.MapTasks {
		if s.tracker.MapTasks[i].State != Completed {
			allMapCompleted = false
			if s.tracker.MapTasks[i].State == Idle {
				s.tracker.MapTasks[i].State = InProgress
				s.tracker.MapTasks[i].WorkerID = workerID
				s.tracker.MapTasks[i].StartTime = time.Now()

				// We return a copy of the task to prevent callers from directly mutating
				// the tracker's internal state without going through the Scheduler APIs
				// and its mutex lock.
				taskCopy := s.tracker.MapTasks[i]
				return &taskCopy, nil
			}
		}
	}

	if !allMapCompleted {
		return nil, ErrNoIdleTasks
	}

	allReduceCompleted := true
	for i := range s.tracker.ReduceTasks {
		if s.tracker.ReduceTasks[i].State != Completed {
			allReduceCompleted = false
			if s.tracker.ReduceTasks[i].State == Idle {
				s.tracker.ReduceTasks[i].State = InProgress
				s.tracker.ReduceTasks[i].WorkerID = workerID
				s.tracker.ReduceTasks[i].StartTime = time.Now()

				// Return a safe copy to prevent data races and unauthorized mutations
				taskCopy := s.tracker.ReduceTasks[i]
				return &taskCopy, nil
			}
		}
	}

	if !allReduceCompleted {
		return nil, ErrNoIdleTasks
	}

	return nil, ErrJobCompleted
}

// CompleteTask marks the given in-progress task as Completed.
// Returns ErrTaskNotFound if taskID doesn't exist, or
// ErrInvalidStateTransition if the task is not currently InProgress.
func (s *Scheduler) CompleteTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	if task.State != InProgress {
		return ErrInvalidStateTransition
	}

	task.State = Completed
	task.WorkerID = ""
	task.StartTime = time.Time{}
	return nil
}

// FailTask marks the given in-progress task as Idle and clears any stale trace
// bindings (such as WorkerID and StartTime).
// Returns ErrTaskNotFound if taskID doesn't exist, or
// ErrInvalidStateTransition if the task is not currently InProgress.
func (s *Scheduler) FailTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	if task.State != InProgress {
		return ErrInvalidStateTransition
	}

	task.State = Idle
	task.WorkerID = ""
	task.StartTime = time.Time{}
	return nil
}

// FailStaleTasks scans all InProgress tasks. If a task has been running longer than
// the given timeout without being completed, it assumes the worker has crashed,
// forcefully transitions the task back to Idle, and returns it to the schedulable pool.
// Returns the number of tasks successfully recovered.
func (s *Scheduler) FailStaleTasks(timeout time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	recoveredCount := 0

	for _, task := range s.taskMap {
		if task.State == InProgress && now.Sub(task.StartTime) > timeout {
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
			recoveredCount++
		}
	}

	return recoveredCount
}

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
)

type Scheduler struct {
	mu      sync.Mutex
	tracker *TaskTracker
	taskMap map[string]*Task // O(1) lookups by task ID
}

func NewScheduler(tracker *TaskTracker) (*Scheduler, error) {
	if tracker == nil {
		return nil, errors.New("tracker cannot be nil")
	}

	taskMap := make(map[string]*Task)

	// Validate MapTasks
	for i := range tracker.MapTasks {
		task := &tracker.MapTasks[i]
		if task.State != Idle {
			return nil, errors.New("all initial tasks must be Idle")
		}
		if _, exists := taskMap[task.ID]; exists {
			return nil, errors.New("duplicate task ID found")
		}
		taskMap[task.ID] = task
	}

	// Validate ReduceTasks
	for i := range tracker.ReduceTasks {
		task := &tracker.ReduceTasks[i]
		if task.State != Idle {
			return nil, errors.New("all initial tasks must be Idle")
		}
		if _, exists := taskMap[task.ID]; exists {
			return nil, errors.New("duplicate task ID found")
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

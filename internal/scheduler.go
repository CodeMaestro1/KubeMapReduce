package manager

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNoIdleTasks  = errors.New("no idle tasks available right now, please wait")
	ErrJobCompleted = errors.New("all tasks completed, job is done")
	ErrTaskNotFound = errors.New("task not found")
)

type Scheduler struct {
	mu      sync.Mutex
	tracker *TaskTracker
}

func NewScheduler(tracker *TaskTracker) *Scheduler {
	return &Scheduler{
		tracker: tracker,
	}
}

// GetNextTask returns the next available idle map task.
// If all map tasks are completed, it returns the next available idle reduce task.
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

	for i := range s.tracker.MapTasks {
		if s.tracker.MapTasks[i].ID == taskID {
			s.tracker.MapTasks[i].State = Completed
			return nil
		}
	}

	for i := range s.tracker.ReduceTasks {
		if s.tracker.ReduceTasks[i].ID == taskID {
			s.tracker.ReduceTasks[i].State = Completed
			return nil
		}
	}

	return ErrTaskNotFound
}

func (s *Scheduler) FailTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.tracker.MapTasks {
		if s.tracker.MapTasks[i].ID == taskID {
			s.tracker.MapTasks[i].State = Idle
			s.tracker.MapTasks[i].WorkerID = ""
			return nil
		}
	}

	for i := range s.tracker.ReduceTasks {
		if s.tracker.ReduceTasks[i].ID == taskID {
			s.tracker.ReduceTasks[i].State = Idle
			s.tracker.ReduceTasks[i].WorkerID = ""
			return nil
		}
	}

	return ErrTaskNotFound
}

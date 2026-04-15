// Package manager provides the MapReduce scheduling and task tracking components.
package manager

import (
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const MaxTaskAttempts = 3

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
	ErrEmptyTaskID            = errors.New("task ID cannot be empty")
	ErrInvalidTimeout         = errors.New("timeout must be strictly positive")
	ErrStaleAttempt           = errors.New("stale commit attempt rejected to prevent split-brain")
	ErrExpiredLease           = errors.New("lease expired or mismatched")
	ErrOutputMismatch         = errors.New("outputURIs and outputChecksums must have the same length")
	ErrJobFailed              = errors.New("job failed: one or more tasks reached maximum attempts")
)

// Scheduler coordinates task assignment and lifecycle for a single MapReduce job.

type Scheduler struct {
	mu              sync.Mutex
	tracker         *TaskTracker
	taskMap         map[string]*Task // O(1) lookups by task ID
	mapIdleQueue    []string         // Queue of idle map task IDs
	reduceIdleQueue []string         // Queue of idle reduce task IDs
}

// NewScheduler initializes a Scheduler with O(1) task index mapping.
func NewScheduler(tracker *TaskTracker) (*Scheduler, error) {
	if tracker == nil {
		return nil, ErrNilTracker
	}

	tracker = NewTaskTracker(tracker.mapTasks, tracker.reduceTasks)

	taskMap := make(map[string]*Task)
	var mapIdleQueue []string
	var reduceIdleQueue []string

	// Validate MapTasks
	for i := range tracker.mapTasks {
		task := &tracker.mapTasks[i]
		if strings.TrimSpace(task.ID) == "" {
			return nil, ErrEmptyTaskID
		}
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
		mapIdleQueue = append(mapIdleQueue, task.ID)
	}

	// Validate ReduceTasks
	for i := range tracker.reduceTasks {
		task := &tracker.reduceTasks[i]
		if strings.TrimSpace(task.ID) == "" {
			return nil, ErrEmptyTaskID
		}
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
		reduceIdleQueue = append(reduceIdleQueue, task.ID)
	}

	return &Scheduler{
		tracker:         tracker,
		taskMap:         taskMap,
		mapIdleQueue:    mapIdleQueue,
		reduceIdleQueue: reduceIdleQueue,
	}, nil
}

func (s *Scheduler) GetNextTask(workerID string) (*Task, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, ErrEmptyWorkerID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Check for failed tasks in Map phase
	for i := range s.tracker.mapTasks {
		if s.tracker.mapTasks[i].State == Failed {
			return nil, ErrJobFailed
		}
	}

	// 2. Try to assign Map Tasks from queue
	if len(s.mapIdleQueue) > 0 {
		taskID := s.mapIdleQueue[0]
		s.mapIdleQueue = s.mapIdleQueue[1:]

		task := s.taskMap[taskID]
		task.State = InProgress
		task.workerID = workerID
		task.startTime = time.Now()
		task.LastHeartbeat = time.Now()
		task.ActiveAttemptID = uuid.NewString()
		task.LeaseID = uuid.NewString()

		taskCopy := task.Clone()
		return &taskCopy, nil
	}

	// 3. If Map phase is not finished, worker must wait
	for i := range s.tracker.mapTasks {
		if s.tracker.mapTasks[i].State != Completed {
			return nil, ErrNoIdleTasks
		}
	}

	// 4. Check for failed tasks in Reduce phase
	for i := range s.tracker.reduceTasks {
		if s.tracker.reduceTasks[i].State == Failed {
			return nil, ErrJobFailed
		}
	}

	// 5. Try to assign Reduce Tasks from queue
	if len(s.reduceIdleQueue) > 0 {
		taskID := s.reduceIdleQueue[0]
		s.reduceIdleQueue = s.reduceIdleQueue[1:]

		task := s.taskMap[taskID]
		task.State = InProgress
		task.workerID = workerID
		task.startTime = time.Now()
		task.LastHeartbeat = time.Now()
		task.ActiveAttemptID = uuid.NewString()
		task.LeaseID = uuid.NewString()

		taskCopy := task.Clone()
		return &taskCopy, nil
	}

	// 6. If Reduce phase is not finished, worker must wait
	for i := range s.tracker.reduceTasks {
		if s.tracker.reduceTasks[i].State != Completed {
			return nil, ErrNoIdleTasks
		}
	}

	return nil, ErrJobCompleted
}

func (s *Scheduler) CompleteTask(taskID string, attemptID string, outputURIs []string, outputChecksums []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	if task.State != InProgress {
		return ErrInvalidStateTransition
	}

	if task.ActiveAttemptID != attemptID {
		return ErrStaleAttempt
	}

	// Guard against 1NF corruption: each output URI must pair with exactly one checksum
	// when both slices are provided (Section 5.5 of Design Document).
	if len(outputURIs) > 0 && len(outputChecksums) > 0 && len(outputURIs) != len(outputChecksums) {
		return ErrOutputMismatch
	}

	task.State = Completed
	task.OutputURIs = outputURIs
	task.OutputChecksums = outputChecksums

	task.workerID = ""
	task.startTime = time.Time{}
	task.LastHeartbeat = time.Time{}
	task.ActiveAttemptID = ""
	task.LeaseID = ""
	return nil
}

func (s *Scheduler) GetMapOutputs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var outputs []string
	for i := range s.tracker.mapTasks {
		if s.tracker.mapTasks[i].State == Completed {
			outputs = append(outputs, s.tracker.mapTasks[i].OutputURIs...)
		}
	}
	return outputs
}

// GetReduceOutputs collects all output URIs from completed Reduce tasks.
// Used by the Manager to build the final result set for the Retrieve Result flow.
func (s *Scheduler) GetReduceOutputs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var outputs []string
	for i := range s.tracker.reduceTasks {
		if s.tracker.reduceTasks[i].State == Completed {
			outputs = append(outputs, s.tracker.reduceTasks[i].OutputURIs...)
		}
	}
	return outputs
}

// AllMapTasksCompleted returns true when every map task has reached
// the Completed state. The Manager uses this to decide when to transition
// the job from the Map phase into the Reduce phase.
func (s *Scheduler) AllMapTasksCompleted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.tracker.mapTasks {
		if s.tracker.mapTasks[i].State != Completed {
			return false
		}
	}
	return true
}

// IsJobFinished returns true when every map AND every reduce task has
// reached the Completed state. The Manager uses this to transition the
// job to Completed (or Cleaning).
func (s *Scheduler) IsJobFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.tracker.mapTasks {
		if s.tracker.mapTasks[i].State != Completed {
			return false
		}
	}
	for i := range s.tracker.reduceTasks {
		if s.tracker.reduceTasks[i].State != Completed {
			return false
		}
	}
	return true
}

// GetTaskStatus provides a read-only query for a specific task's current state.
// This supports the UI Service's CQRS-style read path for the jobs status CLI command.
func (s *Scheduler) GetTaskStatus(taskID string) (TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return 0, ErrTaskNotFound
	}
	return task.State, nil
}

// GetTaskByID returns a full defensive copy of a task's metadata.
// The Manager uses this for the gRPC Register handler to build a TaskAssignment
// containing byte ranges, code URIs, checksums, and lease info.
func (s *Scheduler) GetTaskByID(taskID string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return nil, ErrTaskNotFound
	}

	taskCopy := task.Clone()
	return &taskCopy, nil
}

func (s *Scheduler) RenewLease(taskID string, attemptID string, leaseID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	if task.State != InProgress {
		return ErrInvalidStateTransition
	}

	if task.ActiveAttemptID != attemptID {
		return ErrStaleAttempt
	}

	if task.LeaseID != leaseID {
		return ErrExpiredLease
	}

	task.LastHeartbeat = time.Now()
	return nil
}

// FailTask transitions an InProgress task back to Idle (or Failed after
// exhausting MaxTaskAttempts). The attemptID and leaseID parameters implement
// the "Zombie Fencing" protocol described in Section 5.1 of the design
// document: the Manager rejects stale failures from superseded attempts
// or expired leases, preventing split-brain state corruption.
func (s *Scheduler) FailTask(taskID string, attemptID string, leaseID string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[taskID]
	if !exists {
		return ErrTaskNotFound
	}

	if task.State != InProgress {
		return ErrInvalidStateTransition
	}

	if task.ActiveAttemptID != attemptID {
		return ErrStaleAttempt
	}

	if task.LeaseID != leaseID {
		return ErrExpiredLease
	}

	task.Attempts++
	task.LastFailure = reason
	task.LastFailedAt = time.Now()

	if task.Attempts >= MaxTaskAttempts {
		task.State = Failed
	} else {
		task.State = Idle
		if task.Type == MapTask {
			s.mapIdleQueue = append(s.mapIdleQueue, task.ID)
		} else {
			s.reduceIdleQueue = append(s.reduceIdleQueue, task.ID)
		}
	}

	task.workerID = ""
	task.startTime = time.Time{}
	task.LastHeartbeat = time.Time{}
	task.ActiveAttemptID = ""
	task.LeaseID = ""
	return nil
}

func (s *Scheduler) FailStaleTasks(timeout time.Duration) (int, error) {
	if timeout <= 0 {
		return 0, ErrInvalidTimeout
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	recoveredCount := 0

	for _, task := range s.taskMap {
		if task.State == InProgress {
			if task.startTime.IsZero() {
				log.Printf("CRITICAL: task %s in impossible state (InProgress with zero startTime)\n", task.ID)
				continue
			}

			if now.Sub(task.LastHeartbeat) > timeout {
				task.Attempts++
				task.LastFailure = "stale timeout"
				task.LastFailedAt = now

				if task.Attempts >= MaxTaskAttempts {
					task.State = Failed
				} else {
					task.State = Idle
					if task.Type == MapTask {
						s.mapIdleQueue = append(s.mapIdleQueue, task.ID)
					} else {
						s.reduceIdleQueue = append(s.reduceIdleQueue, task.ID)
					}
				}

				task.workerID = ""
				task.startTime = time.Time{}
				task.LastHeartbeat = time.Time{}
				task.ActiveAttemptID = ""
				task.LeaseID = ""
				recoveredCount++
			}
		}
	}

	return recoveredCount, nil
}

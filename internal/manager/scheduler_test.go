package manager

import (
	"testing"
	"time"
)

func TestScheduler_MapBeforeReduce(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
		ReduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Step 1: Assign first map task
	task1, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}
	if task1.ID != "m1" {
		t.Errorf("expected task m1, got %s", task1.ID)
	}

	// Step 2: Assign second map task
	task2, err := scheduler.GetNextTask("worker-2")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}
	if task2.ID != "m2" {
		t.Errorf("expected task m2, got %s", task2.ID)
	}

	// Step 3: No idle map tasks, reduce tasks should wait because map tasks aren't done
	_, err = scheduler.GetNextTask("worker-3")
	if err != ErrNoIdleTasks {
		t.Errorf("expected ErrNoIdleTasks, got %v", err)
	}

	// Step 4: Complete m1
	err = scheduler.CompleteTask("m1")
	if err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Step 5: Still waiting for m2
	_, err = scheduler.GetNextTask("worker-3")
	if err != ErrNoIdleTasks {
		t.Errorf("expected ErrNoIdleTasks, got %v", err)
	}

	// Step 6: Complete m2
	err = scheduler.CompleteTask("m2")
	if err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Step 7: Now reduce task should be assigned
	task3, err := scheduler.GetNextTask("worker-3")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}
	if task3.ID != "r1" {
		t.Errorf("expected task r1, got %s", task3.ID)
	}

	// Step 8: Complete r1
	err = scheduler.CompleteTask("r1")
	if err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Step 9: Job should be completed
	_, err = scheduler.GetNextTask("worker-4")
	if err != ErrJobCompleted {
		t.Errorf("expected ErrJobCompleted, got %v", err)
	}
}

func TestScheduler_FailTask(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Assign task
	_, err = scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	// Fail task
	err = scheduler.FailTask("m1")
	if err != nil {
		t.Fatalf("unexpected error failing task: %v", err)
	}

	// Task should be available again
	task2, err := scheduler.GetNextTask("worker-2")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}
	if task2.ID != "m1" {
		t.Errorf("expected task m1, got %s", task2.ID)
	}
	if task2.WorkerID != "worker-2" {
		t.Errorf("expected worker-2, got %s", task2.WorkerID)
	}
}

// Completing an Idle task should be an invalid state transition.
func TestScheduler_CompleteIdleTask_InvalidTransition(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Directly completing an Idle task (without assignment) should fail.
	err = scheduler.CompleteTask("m1")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition when completing Idle task, got %v", err)
	}
}

// Failing a Completed task should be an invalid state transition.
func TestScheduler_FailCompletedTask_InvalidTransition(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Assign task and complete it successfully.
	_, err = scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task assignment, got err: %v", err)
	}

	if err := scheduler.CompleteTask("m1"); err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Now failing a Completed task should not be allowed.
	err = scheduler.FailTask("m1")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition when failing Completed task, got %v", err)
	}
}

func TestScheduler_NilTracker(t *testing.T) {
	_, err := NewScheduler(nil)
	if err == nil {
		t.Fatalf("expected error when passing nil tracker, got nil")
	}
}

func TestScheduler_DuplicateTaskIDs(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "dup1", Type: MapTask, State: Idle},
		},
		ReduceTasks: []Task{
			{ID: "dup1", Type: ReduceTask, State: Idle}, // duplicate ID
		},
	}

	_, err := NewScheduler(tracker)
	if err == nil {
		t.Fatalf("expected error when tasks have duplicate IDs, got nil")
	}
}

func TestScheduler_InitialStateValidation(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: InProgress}, // invalid initial state
		},
	}

	_, err := NewScheduler(tracker)
	if err == nil {
		t.Fatalf("expected error when initial task state is not Idle, got nil")
	}
}

func TestScheduler_Concurrency(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
			{ID: "m3", Type: MapTask, State: Idle},
			{ID: "m4", Type: MapTask, State: Idle},
			{ID: "m5", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Spin up 10 concurrent goroutines racing to get or complete tasks
	errCh := make(chan error, 100)
	doneCh := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(workerID int) {
			for {
				task, err := scheduler.GetNextTask("worker-X")
				if err == ErrNoIdleTasks {
					time.Sleep(10 * time.Millisecond) // Don't burn CPU in busy-wait
					continue
				}
				if err == ErrJobCompleted {
					doneCh <- true
					return
				}
				if err != nil {
					errCh <- err
					return
				}

				// Complete the task
				if err := scheduler.CompleteTask(task.ID); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}

	// Wait for 10 goroutines to finish (either success or hit job completed)
	for i := 0; i < 10; i++ {
		select {
		case err := <-errCh:
			t.Fatalf("concurrent worker failed: %v", err)
		case <-doneCh:
			// Success
		}
	}
}

func TestScheduler_EmptyWorkerID(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	_, err = scheduler.GetNextTask("")
	if err != ErrEmptyWorkerID {
		t.Fatalf("expected ErrEmptyWorkerID when assigning task with empty worker ID, got: %v", err)
	}
}

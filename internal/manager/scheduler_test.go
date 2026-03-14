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
	doneCh := make(chan bool, 10) // Buffalo to prevent zombie blocking

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

func TestScheduler_InvalidTaskType(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: ReduceTask, State: Idle}, // Placed in MapTasks array but typed as ReduceTask
		},
	}

	_, err := NewScheduler(tracker)
	if err != ErrInvalidTaskType {
		t.Fatalf("expected ErrInvalidTaskType when providing mismatched task struct types, got: %v", err)
	}
}

func TestScheduler_FailStaleTasks(t *testing.T) {
	tracker := &TaskTracker{
		MapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// Request the task which will log its `StartTime` and assign it to a worker.
	_, err = scheduler.GetNextTask("crashing-worker")
	if err != nil {
		t.Fatalf("expected to get task properly, got err: %v", err)
	}

	// FailStaleTasks immediately with a 10s boundary should yield 0 recoveries
	recovered := scheduler.FailStaleTasks(10 * time.Second)
	if recovered != 0 {
		t.Fatalf("expected no tasks recovered initially, got %d", recovered)
	}

	// Sleep 15ms so the recorded StartTime drops into the past.
	time.Sleep(15 * time.Millisecond)

	// Recover elements that have been processing for more than 5 milliseconds
	recovered = scheduler.FailStaleTasks(5 * time.Millisecond)
	if recovered != 1 {
		t.Fatalf("expected exactly 1 stale task to be recovered, got %d", recovered)
	}

	// Now that it's recovered and "Idle", another worker should be able to get it immediately
	task, err := scheduler.GetNextTask("healthy-worker")
	if err != nil {
		t.Fatalf("expected healthy worker to acquire recovered task, got err: %v", err)
	}
	if task.ID != "m1" || task.WorkerID != "healthy-worker" {
		t.Fatalf("expected m1 to be assigned to healthy-worker, got %#v", task)
	}
}

// Proof-of-concept: mutating tracker slices after NewScheduler can desync
// scheduler.taskMap pointers from the current tracker storage.
func TestScheduler_ExternalTrackerMutation_BreaksTaskMapIndex(t *testing.T) {
	// Capacity is intentionally 1 so append triggers reallocation.
	mapTasks := make([]Task, 1, 1)
	mapTasks[0] = Task{ID: "m1", Type: MapTask, State: Idle}

	tracker := &TaskTracker{
		MapTasks: mapTasks,
	}

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// External mutation after scheduler creation: this reallocates MapTasks.
	tracker.MapTasks = append(tracker.MapTasks, Task{ID: "m2", Type: MapTask, State: Idle})

	// Scheduler assigns m1 from the new slice backing array and marks it InProgress.
	task, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task assignment, got err: %v", err)
	}
	if task.ID != "m1" {
		t.Fatalf("expected m1, got %s", task.ID)
	}

	// CompleteTask uses taskMap pointer captured before reallocation.
	// That stale pointer still sees m1 as Idle, so transition fails.
	err = scheduler.CompleteTask("m1")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition due to stale pointer desync, got: %v", err)
	}
}

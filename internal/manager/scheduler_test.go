package manager

import (
	"testing"
	"time"
)

func TestScheduler_MapBeforeReduce(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
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
		mapTasks: []Task{
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
	err = scheduler.FailTask("m1", "worker crashed")
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
	if task2.WorkerID() != "worker-2" {
		t.Errorf("expected worker-2, got %s", task2.WorkerID())
	}
}

// Completing an Idle task should be an invalid state transition.
func TestScheduler_CompleteIdleTask_InvalidTransition(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
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
		mapTasks: []Task{
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
	err = scheduler.FailTask("m1", "worker crashed")
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
		mapTasks: []Task{
			{ID: "dup1", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
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
		mapTasks: []Task{
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
		mapTasks: []Task{
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
		mapTasks: []Task{
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
	
	_, err = scheduler.GetNextTask("   \t   ")
	if err != ErrEmptyWorkerID {
		t.Fatalf("expected ErrEmptyWorkerID when assigning task with whitespace worker ID, got: %v", err)
	}
}

func TestScheduler_EmptyTaskID(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "   ", Type: MapTask, State: Idle},
		},
	}

	_, err := NewScheduler(tracker)
	if err != ErrEmptyTaskID {
		t.Fatalf("expected ErrEmptyTaskID, got %v", err)
	}
}

func TestScheduler_InvalidTaskType(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: ReduceTask, State: Idle}, // Placed in mapTasks array but typed as ReduceTask
		},
	}

	_, err := NewScheduler(tracker)
	if err != ErrInvalidTaskType {
		t.Fatalf("expected ErrInvalidTaskType when providing mismatched task struct types, got: %v", err)
	}
}

func TestScheduler_FailStaleTasks(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
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
	recovered, _ := scheduler.FailStaleTasks(10 * time.Second)
	if recovered != 0 {
		t.Fatalf("expected no tasks recovered initially, got %d", recovered)
	}

	// Sleep 15ms so the recorded StartTime drops into the past.
	time.Sleep(15 * time.Millisecond)

	// Recover elements that have been processing for more than 5 milliseconds
	recovered, _ = scheduler.FailStaleTasks(5 * time.Millisecond)
	if recovered != 1 {
		t.Fatalf("expected exactly 1 stale task to be recovered, got %d", recovered)
	}

	// Now that it's recovered and "Idle", another worker should be able to get it immediately
	task, err := scheduler.GetNextTask("healthy-worker")
	if err != nil {
		t.Fatalf("expected healthy worker to acquire recovered task, got err: %v", err)
	}
	if task.ID != "m1" || task.WorkerID() != "healthy-worker" {
		t.Fatalf("expected m1 to be assigned to healthy-worker, got %#v", task)
	}
}

// Proof-of-concept: mutating tracker slices after NewScheduler can desync
// scheduler.taskMap pointers from the current tracker storage.
// Proof-of-concept: mutating tracker slices after NewTaskTracker won't desync
// scheduler.taskMap pointers because NewTaskTracker makes defensive copies.
func TestScheduler_ExternalTrackerMutation_NoDesync(t *testing.T) {
	mapTasks := make([]Task, 1)
	mapTasks[0] = Task{ID: "m1", Type: MapTask, State: Idle}

	tracker := NewTaskTracker(mapTasks, nil)

	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	// External mutation after scheduler creation: mutates the tracker's array
	tracker.mapTasks[0].State = Completed
	tracker.mapTasks = append(tracker.mapTasks, Task{ID: "m2", Type: MapTask, State: Idle})

	// Scheduler assigns m1 safely from its internal disconnected tracker
	task, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task assignment, got err: %v", err)
	}
	if task.ID != "m1" {
		t.Fatalf("expected m1, got %s", task.ID)
	}

	err = scheduler.CompleteTask("m1")
	if err != nil {
		t.Fatalf("expected no error since state is safely maintained, got: %v", err)
	}
}

func TestScheduler_MaxTaskAttempts(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	for i := 0; i < MaxTaskAttempts; i++ {
		_, err := scheduler.GetNextTask(func() string { return "w" }())
		if err != nil {
			t.Fatalf("iteration %d: expected task assignment, got %v", i, err)
		}
		err = scheduler.FailTask("m1", "failed")
		if err != nil {
			t.Fatalf("iteration %d: unexpected error failing task: %v", i, err)
		}
	}

	// Now it should be Failed, not Idle. GetNextTask should not return it.
	_, err := scheduler.GetNextTask("w")
	if err != ErrNoIdleTasks {
		t.Fatalf("expected ErrNoIdleTasks after max attempts, got: %v", err)
	}

	sTask := scheduler.taskMap["m1"]
	if sTask.State != Failed {
		t.Fatalf("expected task state to be Failed, got: %v", sTask.State)
	}
}

func TestScheduler_FailStaleTasks_ImpossibleState(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Artificially create impossible state
	scheduler.taskMap["m1"].State = InProgress
	scheduler.taskMap["m1"].startTime = time.Time{}

	recovered, _ := scheduler.FailStaleTasks(1 * time.Millisecond)
	if recovered != 0 {
		t.Fatalf("expected 0 recoveries due to guard, got %d", recovered)
	}
	if scheduler.taskMap["m1"].State != InProgress {
		t.Fatalf("expected task to remain InProgress, got %v", scheduler.taskMap["m1"].State)
	}
}


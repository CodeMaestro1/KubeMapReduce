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
	err = scheduler.CompleteTask("m1", task1.GetAttemptID(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Step 5: Still waiting for m2
	_, err = scheduler.GetNextTask("worker-3")
	if err != ErrNoIdleTasks {
		t.Errorf("expected ErrNoIdleTasks, got %v", err)
	}

	// Step 6: Complete m2
	err = scheduler.CompleteTask("m2", task2.GetAttemptID(), nil, nil)
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
	err = scheduler.CompleteTask("r1", task3.GetAttemptID(), nil, nil)
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
	task1, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	// Fail task
	err = scheduler.FailTask("m1", task1.GetAttemptID(), task1.GetLeaseID(), "worker crashed")
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
	err = scheduler.CompleteTask("m1", "some-attempt-id", nil, nil)
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
	assignedTask, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task assignment, got err: %v", err)
	}

	if err := scheduler.CompleteTask("m1", assignedTask.GetAttemptID(), nil, nil); err != nil {
		t.Fatalf("unexpected error completing task: %v", err)
	}

	// Now failing a Completed task should not be allowed.
	err = scheduler.FailTask("m1", assignedTask.GetAttemptID(), assignedTask.GetLeaseID(), "worker crashed")
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
				if err := scheduler.CompleteTask(task.ID, task.GetAttemptID(), nil, nil); err != nil {
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

	err = scheduler.CompleteTask("m1", task.GetAttemptID(), nil, nil)
	if err != nil {
		t.Fatalf("expected no error since state is safely maintained, got: %v", err)
	}
}

func TestScheduler_RenewLease(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("worker-1")
	initialHeartbeat := task.GetHeartbeat()

	time.Sleep(5 * time.Millisecond)

	err := scheduler.RenewLease("m1", task.GetAttemptID(), task.GetLeaseID())
	if err != nil {
		t.Fatalf("unexpected error renewing lease: %v", err)
	}

	taskRef := scheduler.taskMap["m1"]
	if taskRef.GetHeartbeat().Sub(initialHeartbeat) <= 0 {
		t.Fatalf("expected LastHeartbeat to be updated, got %v (initial: %v)", taskRef.GetHeartbeat(), initialHeartbeat)
	}

	// Try renewing with bad lease
	err = scheduler.RenewLease("m1", task.GetAttemptID(), "bad-lease-id")
	if err != ErrExpiredLease {
		t.Fatalf("expected ErrExpiredLease, got %v", err)
	}
}

func TestScheduler_StaleAttempt(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("worker-1")

	// Zombie worker attempting to commit with bad attempt ID
	err := scheduler.CompleteTask("m1", "old-zombie-attempt-id", nil, nil)
	if err != ErrStaleAttempt {
		t.Fatalf("expected ErrStaleAttempt for mismatching attempt ID, got: %v", err)
	}

	// Correct attempt commit
	err = scheduler.CompleteTask("m1", task.GetAttemptID(), []string{"s3://out1"}, []string{"hash1"})
	if err != nil {
		t.Fatalf("expected successful complete, got: %v", err)
	}
}

func TestScheduler_GetMapOutputs(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task1, _ := scheduler.GetNextTask("w1")
	_ = scheduler.CompleteTask("m1", task1.GetAttemptID(), []string{"s3://m1-out"}, []string{})

	outputs := scheduler.GetMapOutputs()
	if len(outputs) != 1 || outputs[0] != "s3://m1-out" {
		t.Fatalf("expected one Map Output, got: %v", outputs)
	}

	task2, _ := scheduler.GetNextTask("w2")
	_ = scheduler.CompleteTask("m2", task2.GetAttemptID(), []string{"s3://m2-out-a", "s3://m2-out-b"}, []string{})

	outputs = scheduler.GetMapOutputs()
	if len(outputs) != 3 {
		t.Fatalf("expected three combined Map Outputs, got: %d", len(outputs))
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
		task, err := scheduler.GetNextTask(func() string { return "w" }())
		if err != nil {
			t.Fatalf("iteration %d: expected task assignment, got %v", i, err)
		}
		err = scheduler.FailTask("m1", task.GetAttemptID(), task.GetLeaseID(), "failed")
		if err != nil {
			t.Fatalf("iteration %d: unexpected error failing task: %v", i, err)
		}
	}

	// Now it should be Failed, not Idle. GetNextTask should not return it.
	_, err := scheduler.GetNextTask("w")
	if err != ErrJobFailed {
		t.Fatalf("expected ErrJobFailed after max attempts, got: %v", err)
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

func TestScheduler_AllMapTasksCompleted(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	if scheduler.AllMapTasksCompleted() {
		t.Fatal("expected AllMapTasksCompleted=false before any work")
	}

	task1, _ := scheduler.GetNextTask("w1")
	_ = scheduler.CompleteTask("m1", task1.GetAttemptID(), nil, nil)

	if scheduler.AllMapTasksCompleted() {
		t.Fatal("expected AllMapTasksCompleted=false with m2 still pending")
	}

	task2, _ := scheduler.GetNextTask("w2")
	_ = scheduler.CompleteTask("m2", task2.GetAttemptID(), nil, nil)

	if !scheduler.AllMapTasksCompleted() {
		t.Fatal("expected AllMapTasksCompleted=true after completing all map tasks")
	}
}

func TestScheduler_IsJobFinished(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	if scheduler.IsJobFinished() {
		t.Fatal("expected IsJobFinished=false at start")
	}

	task1, _ := scheduler.GetNextTask("w1")
	_ = scheduler.CompleteTask("m1", task1.GetAttemptID(), nil, nil)

	if scheduler.IsJobFinished() {
		t.Fatal("expected IsJobFinished=false — reduce not done")
	}

	task2, _ := scheduler.GetNextTask("w2")
	_ = scheduler.CompleteTask("r1", task2.GetAttemptID(), nil, nil)

	if !scheduler.IsJobFinished() {
		t.Fatal("expected IsJobFinished=true after all tasks complete")
	}
}

func TestScheduler_GetTaskStatus(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	state, err := scheduler.GetTaskStatus("m1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != Idle {
		t.Fatalf("expected Idle, got %v", state)
	}

	task, _ := scheduler.GetNextTask("w1")
	state, _ = scheduler.GetTaskStatus("m1")
	if state != InProgress {
		t.Fatalf("expected InProgress, got %v", state)
	}

	_ = scheduler.CompleteTask("m1", task.GetAttemptID(), nil, nil)
	state, _ = scheduler.GetTaskStatus("m1")
	if state != Completed {
		t.Fatalf("expected Completed, got %v", state)
	}

	_, err = scheduler.GetTaskStatus("nonexistent")
	if err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestScheduler_GetReduceOutputs(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
			{ID: "r2", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Complete the map phase first
	task1, _ := scheduler.GetNextTask("w1")
	_ = scheduler.CompleteTask("m1", task1.GetAttemptID(), []string{"s3://m1-out"}, nil)

	// No reduce outputs yet
	outputs := scheduler.GetReduceOutputs()
	if len(outputs) != 0 {
		t.Fatalf("expected 0 reduce outputs, got %d", len(outputs))
	}

	// Complete reduce tasks
	r1, _ := scheduler.GetNextTask("w2")
	_ = scheduler.CompleteTask("r1", r1.GetAttemptID(), []string{"s3://r1-final"}, []string{"hash1"})

	outputs = scheduler.GetReduceOutputs()
	if len(outputs) != 1 || outputs[0] != "s3://r1-final" {
		t.Fatalf("expected [s3://r1-final], got %v", outputs)
	}

	r2, _ := scheduler.GetNextTask("w3")
	_ = scheduler.CompleteTask("r2", r2.GetAttemptID(), []string{"s3://r2-a", "s3://r2-b"}, []string{"h1", "h2"})

	outputs = scheduler.GetReduceOutputs()
	if len(outputs) != 3 {
		t.Fatalf("expected 3 reduce outputs, got %d", len(outputs))
	}
}

func TestScheduler_InvalidTimeout(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	_, err := scheduler.FailStaleTasks(0)
	if err != ErrInvalidTimeout {
		t.Fatalf("expected ErrInvalidTimeout for zero timeout, got %v", err)
	}

	_, err = scheduler.FailStaleTasks(-1 * time.Second)
	if err != ErrInvalidTimeout {
		t.Fatalf("expected ErrInvalidTimeout for negative timeout, got %v", err)
	}
}

func TestScheduler_FailTask_NotFound(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	err := scheduler.FailTask("nonexistent", "some-attempt", "some-lease", "crash")
	if err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestScheduler_RenewLease_NotFound(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	err := scheduler.RenewLease("nonexistent", "dummy-attempt", "some-lease")
	if err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestScheduler_CompleteTask_NotFound(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	err := scheduler.CompleteTask("nonexistent", "some-attempt", nil, nil)
	if err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

// Failing an Idle task is not a valid transition (only InProgress can be failed).
func TestScheduler_FailIdleTask_InvalidTransition(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	err := scheduler.FailTask("m1", "any", "any", "some reason")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition when failing an Idle task, got %v", err)
	}
}

// Completing an already-Failed task should be rejected.
func TestScheduler_CompleteFailedTask_InvalidTransition(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Exhaust all retry attempts to move the task into the Failed state
	for i := 0; i < MaxTaskAttempts; i++ {
		task, err := scheduler.GetNextTask("w")
		if err != nil {
			t.Fatalf("iteration %d: expected task, got %v", i, err)
		}
		if err := scheduler.FailTask("m1", task.GetAttemptID(), task.GetLeaseID(), "crash"); err != nil {
			t.Fatalf("iteration %d: unexpected fail error: %v", i, err)
		}
	}

	// Verify the task is now in Failed state
	state, _ := scheduler.GetTaskStatus("m1")
	if state != Failed {
		t.Fatalf("expected task to be in Failed state, got %v", state)
	}

	// Attempting to complete a Failed task should be invalid
	err := scheduler.CompleteTask("m1", "some-attempt", nil, nil)
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition when completing Failed task, got %v", err)
	}
}

// Renewing a lease on an Idle task (not yet assigned) should be invalid.
func TestScheduler_RenewLease_IdleTask(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	err := scheduler.RenewLease("m1", "dummy-attempt", "some-lease")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition for renewing lease on Idle task, got %v", err)
	}
}

// Renewing a lease on a Completed task should be invalid.
func TestScheduler_RenewLease_CompletedTask(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("w1")
	_ = scheduler.CompleteTask("m1", task.GetAttemptID(), nil, nil)

	err := scheduler.RenewLease("m1", task.GetAttemptID(), "any-lease")
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition for renewing lease on Completed task, got %v", err)
	}
}

// FailStaleTasks should transition a task to Failed when MaxTaskAttempts is exhausted.
func TestScheduler_FailStaleTasks_MaxAttemptsExhaustion(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Assign, let it go stale, and recover repeatedly until max attempts
	for i := 0; i < MaxTaskAttempts; i++ {
		_, err := scheduler.GetNextTask("w")
		if err != nil {
			t.Fatalf("iteration %d: expected task, got %v", i, err)
		}

		// Force the heartbeat into the past so FailStaleTasks picks it up
		scheduler.taskMap["m1"].LastHeartbeat = time.Now().Add(-1 * time.Hour)

		recovered, err := scheduler.FailStaleTasks(1 * time.Millisecond)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if recovered != 1 {
			t.Fatalf("iteration %d: expected 1 recovery, got %d", i, recovered)
		}
	}

	// Task should now be in Failed state (not Idle)
	state, _ := scheduler.GetTaskStatus("m1")
	if state != Failed {
		t.Fatalf("expected task to reach Failed after %d stale timeouts, got %v", MaxTaskAttempts, state)
	}

	// GetNextTask should no longer return this task
	_, err := scheduler.GetNextTask("w")
	if err != ErrJobFailed {
		t.Fatalf("expected ErrJobFailed after exhausting max attempts via stale recovery, got %v", err)
	}
}

// When all tasks are Failed, GetNextTask should return ErrNoIdleTasks.
func TestScheduler_GetNextTask_AllFailed(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Exhaust retries
	for i := 0; i < MaxTaskAttempts; i++ {
		task, err := scheduler.GetNextTask("w")
		if err != nil {
			t.Fatalf("iteration %d: expected task, got %v", i, err)
		}
		_ = scheduler.FailTask("m1", task.GetAttemptID(), task.GetLeaseID(), "crash")
	}

	_, err := scheduler.GetNextTask("w")
	if err != ErrJobFailed {
		t.Fatalf("expected ErrJobFailed when all tasks failed, got %v", err)
	}
}

// An empty scheduler (no map or reduce tasks) should immediately return ErrJobCompleted.
func TestScheduler_EmptyTasks(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks:    []Task{},
		reduceTasks: []Task{},
	}
	scheduler, _ := NewScheduler(tracker)

	_, err := scheduler.GetNextTask("w")
	if err != ErrJobCompleted {
		t.Fatalf("expected ErrJobCompleted for empty scheduler, got %v", err)
	}

	if !scheduler.IsJobFinished() {
		t.Fatal("expected IsJobFinished=true for empty scheduler")
	}

	if !scheduler.AllMapTasksCompleted() {
		t.Fatal("expected AllMapTasksCompleted=true for empty scheduler")
	}
}

// GetTaskByID should return a full snapshot of the task or ErrTaskNotFound.
func TestScheduler_GetTaskByID(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle, CodeURI: "s3://code/mapper.py", InputFile: "input.jsonl", ByteStart: 0, ByteEnd: 1024},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Not found
	_, err := scheduler.GetTaskByID("nonexistent")
	if err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}

	// Happy path: should return all metadata
	task, err := scheduler.GetTaskByID("m1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.ID != "m1" {
		t.Fatalf("expected ID m1, got %s", task.ID)
	}
	if task.CodeURI != "s3://code/mapper.py" {
		t.Fatalf("expected CodeURI s3://code/mapper.py, got %s", task.CodeURI)
	}
	if task.ByteEnd != 1024 {
		t.Fatalf("expected ByteEnd 1024, got %d", task.ByteEnd)
	}

	// After assignment, should reflect InProgress state and lease metadata
	assigned, _ := scheduler.GetNextTask("w1")
	task2, _ := scheduler.GetTaskByID("m1")
	if task2.State != InProgress {
		t.Fatalf("expected InProgress after assignment, got %v", task2.State)
	}
	if task2.GetLeaseID() == "" {
		t.Fatal("expected non-empty LeaseID after assignment")
	}
	if task2.GetAttemptID() != assigned.GetAttemptID() {
		t.Fatalf("expected attempt IDs to match: %s vs %s", task2.GetAttemptID(), assigned.GetAttemptID())
	}
}

// GetTaskByID must return a defensive copy so external mutation can't corrupt scheduler state.
func TestScheduler_GetTaskByID_ReturnsDefensiveCopy(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Get a copy and mutate it
	taskCopy, _ := scheduler.GetTaskByID("m1")
	taskCopy.State = Completed
	taskCopy.ID = "MUTATED"

	// Internal state should be unaffected
	original, _ := scheduler.GetTaskByID("m1")
	if original.State != Idle {
		t.Fatalf("expected internal state to remain Idle, got %v", original.State)
	}
	if original.ID != "m1" {
		t.Fatalf("expected internal ID to remain m1, got %s", original.ID)
	}
}

// CompleteTask should reject mismatched URI/checksum lengths to prevent 1NF corruption.
func TestScheduler_CompleteTask_OutputMismatch(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("w1")

	// 2 URIs but 1 checksum → should be rejected
	err := scheduler.CompleteTask("m1", task.GetAttemptID(), []string{"uri1", "uri2"}, []string{"hash1"})
	if err != ErrOutputMismatch {
		t.Fatalf("expected ErrOutputMismatch for mismatched lengths, got %v", err)
	}

	// Equal lengths should succeed
	err = scheduler.CompleteTask("m1", task.GetAttemptID(), []string{"uri1"}, []string{"hash1"})
	if err != nil {
		t.Fatalf("expected successful complete with matching lengths, got %v", err)
	}

	// Nil slices should also succeed (tasks with no outputs like some map phases)
	tracker2 := &TaskTracker{
		mapTasks: []Task{
			{ID: "m2", Type: MapTask, State: Idle},
		},
	}
	scheduler2, _ := NewScheduler(tracker2)
	task2, _ := scheduler2.GetNextTask("w2")
	err = scheduler2.CompleteTask("m2", task2.GetAttemptID(), nil, nil)
	if err != nil {
		t.Fatalf("expected successful complete with nil slices, got %v", err)
	}
}

// Verify the new Task struct fields (CombinerURI, ReplicaIndex, TotalReducers)
// survive the full scheduler lifecycle.
func TestScheduler_CombinerAndReplicaFields(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{
				ID:            "m1",
				Type:          MapTask,
				State:         Idle,
				CombinerURI:   "s3://code/combiner.py",
				ReplicaIndex:  2,
				TotalReducers: 5,
				CodeURI:       "s3://code/mapper.py",
				InputChecksum: "abc123",
			},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Fields should survive NewScheduler's internal copy
	task, err := scheduler.GetTaskByID("m1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.CombinerURI != "s3://code/combiner.py" {
		t.Fatalf("expected CombinerURI 's3://code/combiner.py', got %q", task.CombinerURI)
	}
	if task.ReplicaIndex != 2 {
		t.Fatalf("expected ReplicaIndex 2, got %d", task.ReplicaIndex)
	}
	if task.TotalReducers != 5 {
		t.Fatalf("expected TotalReducers 5, got %d", task.TotalReducers)
	}

	// Fields should survive GetNextTask assignment copy
	assigned, _ := scheduler.GetNextTask("w1")
	if assigned.CombinerURI != "s3://code/combiner.py" {
		t.Fatalf("expected CombinerURI preserved after assignment, got %q", assigned.CombinerURI)
	}
	if assigned.ReplicaIndex != 2 {
		t.Fatalf("expected ReplicaIndex preserved after assignment, got %d", assigned.ReplicaIndex)
	}
	if assigned.TotalReducers != 5 {
		t.Fatalf("expected TotalReducers preserved after assignment, got %d", assigned.TotalReducers)
	}
	if assigned.InputChecksum != "abc123" {
		t.Fatalf("expected InputChecksum preserved after assignment, got %q", assigned.InputChecksum)
	}
}

// FailTask must zero out all transient scheduling metadata so a subsequent
// GetNextTask re-assignment starts cleanly with fresh lease/attempt/heartbeat.
func TestScheduler_FailTask_ClearsMetadata(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	if err := scheduler.FailTask("m1", task.GetAttemptID(), task.GetLeaseID(), "pod died"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ref := scheduler.taskMap["m1"]
	if ref.WorkerID() != "" {
		t.Fatalf("expected workerID cleared, got %q", ref.WorkerID())
	}
	if !ref.StartTime().IsZero() {
		t.Fatalf("expected startTime zeroed, got %v", ref.StartTime())
	}
	if !ref.GetHeartbeat().IsZero() {
		t.Fatalf("expected LastHeartbeat zeroed, got %v", ref.GetHeartbeat())
	}
	if ref.GetAttemptID() != "" {
		t.Fatalf("expected ActiveAttemptID cleared, got %q", ref.GetAttemptID())
	}
	if ref.GetLeaseID() != "" {
		t.Fatalf("expected LeaseID cleared, got %q", ref.GetLeaseID())
	}
}

// CompleteTask must zero out all transient metadata so completed tasks
// don't carry stale lease/worker references into the result set.
func TestScheduler_CompleteTask_ClearsMetadata(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("worker-1")
	if err := scheduler.CompleteTask("m1", task.GetAttemptID(), []string{"s3://out"}, []string{"hash1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ref := scheduler.taskMap["m1"]
	if ref.WorkerID() != "" {
		t.Fatalf("expected workerID cleared, got %q", ref.WorkerID())
	}
	if !ref.StartTime().IsZero() {
		t.Fatalf("expected startTime zeroed, got %v", ref.StartTime())
	}
	if !ref.GetHeartbeat().IsZero() {
		t.Fatalf("expected LastHeartbeat zeroed, got %v", ref.GetHeartbeat())
	}
	if ref.GetAttemptID() != "" {
		t.Fatalf("expected ActiveAttemptID cleared, got %q", ref.GetAttemptID())
	}
	if ref.GetLeaseID() != "" {
		t.Fatalf("expected LeaseID cleared, got %q", ref.GetLeaseID())
	}

	// But outputs must be preserved
	if len(ref.OutputURIs) != 1 || ref.OutputURIs[0] != "s3://out" {
		t.Fatalf("expected output URI preserved, got %v", ref.OutputURIs)
	}
	if len(ref.OutputChecksums) != 1 || ref.OutputChecksums[0] != "hash1" {
		t.Fatalf("expected output checksum preserved, got %v", ref.OutputChecksums)
	}
}

// Full lifecycle: 3 map tasks produce outputs → 2 reduce tasks consume them and produce final results.
func TestScheduler_FullLifecycle_MultiMapMultiReduce(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
			{ID: "m3", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
			{ID: "r2", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, err := NewScheduler(tracker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Assign and complete all 3 map tasks with partition outputs
	mapOutputs := map[string][]string{
		"m1": {"s3://staging/m1/part-0", "s3://staging/m1/part-1"},
		"m2": {"s3://staging/m2/part-0", "s3://staging/m2/part-1"},
		"m3": {"s3://staging/m3/part-0", "s3://staging/m3/part-1"},
	}
	mapChecksums := map[string][]string{
		"m1": {"h1a", "h1b"},
		"m2": {"h2a", "h2b"},
		"m3": {"h3a", "h3b"},
	}

	for i := 0; i < 3; i++ {
		task, err := scheduler.GetNextTask("map-worker")
		if err != nil {
			t.Fatalf("map assign %d: %v", i, err)
		}
		if task.Type != MapTask {
			t.Fatalf("expected MapTask, got %v", task.Type)
		}
		err = scheduler.CompleteTask(task.ID, task.GetAttemptID(), mapOutputs[task.ID], mapChecksums[task.ID])
		if err != nil {
			t.Fatalf("map complete %s: %v", task.ID, err)
		}
	}

	if !scheduler.AllMapTasksCompleted() {
		t.Fatal("expected AllMapTasksCompleted=true after completing all map tasks")
	}

	// Verify all 6 intermediate outputs are collected
	allMapOutputs := scheduler.GetMapOutputs()
	if len(allMapOutputs) != 6 {
		t.Fatalf("expected 6 map outputs, got %d", len(allMapOutputs))
	}

	// Assign and complete both reduce tasks
	for i := 0; i < 2; i++ {
		task, err := scheduler.GetNextTask("reduce-worker")
		if err != nil {
			t.Fatalf("reduce assign %d: %v", i, err)
		}
		if task.Type != ReduceTask {
			t.Fatalf("expected ReduceTask, got %v", task.Type)
		}
		err = scheduler.CompleteTask(task.ID, task.GetAttemptID(),
			[]string{"s3://final/" + task.ID + "/output.jsonl"},
			[]string{"final-hash-" + task.ID},
		)
		if err != nil {
			t.Fatalf("reduce complete %s: %v", task.ID, err)
		}
	}

	if !scheduler.IsJobFinished() {
		t.Fatal("expected IsJobFinished=true after completing all tasks")
	}

	reduceOutputs := scheduler.GetReduceOutputs()
	if len(reduceOutputs) != 2 {
		t.Fatalf("expected 2 final reduce outputs, got %d", len(reduceOutputs))
	}

	_, err = scheduler.GetNextTask("idle-worker")
	if err != ErrJobCompleted {
		t.Fatalf("expected ErrJobCompleted, got %v", err)
	}
}

// GetMapOutputs on a freshly created scheduler must return nil/empty.
func TestScheduler_GetMapOutputs_Empty(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	outputs := scheduler.GetMapOutputs()
	if len(outputs) != 0 {
		t.Fatalf("expected 0 map outputs before any completion, got %d", len(outputs))
	}
}

// GetReduceOutputs on a freshly created scheduler must return nil/empty.
func TestScheduler_GetReduceOutputs_Empty(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	outputs := scheduler.GetReduceOutputs()
	if len(outputs) != 0 {
		t.Fatalf("expected 0 reduce outputs before any completion, got %d", len(outputs))
	}
}

// Concurrently run the stale-task reaper pattern alongside task assignment
// to verify they don't deadlock or corrupt shared state.
func TestScheduler_ConcurrentFailStaleAndAssign(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
			{ID: "m3", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	errCh := make(chan error, 20)
	doneCh := make(chan bool, 10)

	// Worker goroutines: assign and complete tasks
	for i := 0; i < 3; i++ {
		go func() {
			for {
				task, err := scheduler.GetNextTask("worker")
				if err == ErrNoIdleTasks {
					time.Sleep(5 * time.Millisecond)
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
				if err := scheduler.CompleteTask(task.ID, task.GetAttemptID(), nil, nil); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	// Reaper goroutine: continuously sweep for stale tasks
	go func() {
		for i := 0; i < 50; i++ {
			_, err := scheduler.FailStaleTasks(1 * time.Hour) // large timeout, won't actually recover anything
			if err != nil {
				errCh <- err
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Wait for all workers to finish
	for i := 0; i < 3; i++ {
		select {
		case err := <-errCh:
			t.Fatalf("concurrent operation failed: %v", err)
		case <-doneCh:
			// ok
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for concurrent operations to complete")
		}
	}
}

// FailTask increments Attempts correctly across multiple failure cycles.
func TestScheduler_FailTask_PreservesAttemptCount(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	for i := 1; i <= MaxTaskAttempts; i++ {
		task, err := scheduler.GetNextTask("w")
		if err != nil {
			t.Fatalf("attempt %d: expected task, got %v", i, err)
		}

		if err := scheduler.FailTask("m1", task.GetAttemptID(), task.GetLeaseID(), "crash"); err != nil {
			t.Fatalf("attempt %d: unexpected fail error: %v", i, err)
		}

		ref := scheduler.taskMap["m1"]
		if ref.Attempts != i {
			t.Fatalf("after %d failures, expected Attempts=%d, got %d", i, i, ref.Attempts)
		}

		if ref.LastFailure != "crash" {
			t.Fatalf("expected LastFailure='crash', got %q", ref.LastFailure)
		}

		if ref.LastFailedAt.IsZero() {
			t.Fatalf("expected LastFailedAt to be set, got zero")
		}
	}

	// After MaxTaskAttempts, state must be Failed
	if scheduler.taskMap["m1"].State != Failed {
		t.Fatalf("expected Failed state after %d failures, got %v", MaxTaskAttempts, scheduler.taskMap["m1"].State)
	}
}

// Zombie worker fencing tests

func TestScheduler_FailTask_StaleAttemptRejection(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("worker-1")

	// Attempt to fail with an incorrect attempt ID
	err := scheduler.FailTask("m1", "stale-attempt-id", task.GetLeaseID(), "crash")
	if err != ErrStaleAttempt {
		t.Fatalf("expected ErrStaleAttempt when attempting to fail with stale ID, got %v", err)
	}

	// Double check state isn't mutated
	currentTask, _ := scheduler.GetTaskByID("m1")
	if currentTask.Attempts != 0 {
		t.Fatalf("expected no fail attempts recorded on rejection, got %d", currentTask.Attempts)
	}
}

func TestScheduler_FailTask_ExpiredLeaseRejection(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("worker-1")

	// Attempt to fail with an incorrect lease ID
	err := scheduler.FailTask("m1", task.GetAttemptID(), "expired-lease-id", "crash")
	if err != ErrExpiredLease {
		t.Fatalf("expected ErrExpiredLease when attempting to fail with wrong lease, got %v", err)
	}
}

func TestScheduler_GetNextTask_ReturnsDefensiveCopy(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	taskCopy, _ := scheduler.GetNextTask("w")

	// Mutate the returned task copy
	taskCopy.ID = "MUTATED"
	taskCopy.workerID = "MUTATED"
	taskCopy.State = Completed

	// Internal state should be completely unmodified by external mutation
	internalRef := scheduler.taskMap["m1"]
	if internalRef.ID != "m1" {
		t.Fatalf("expected internal task ID m1, got %s", internalRef.ID)
	}
	if internalRef.State == Completed {
		t.Fatalf("expected internal state to be InProgress, got %v", internalRef.State)
	}
}

func TestScheduler_FailStaleTasks_SetsFailureMetadata(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	_, _ = scheduler.GetNextTask("worker")

	// Force task into the past to make it stale
	scheduler.taskMap["m1"].LastHeartbeat = time.Now().Add(-1 * time.Hour)

	recovered, _ := scheduler.FailStaleTasks(1 * time.Minute)
	if recovered != 1 {
		t.Fatalf("expected 1 recovered task, got %d", recovered)
	}

	taskRef := scheduler.taskMap["m1"]
	if taskRef.LastFailure != "stale timeout" {
		t.Fatalf("expected LastFailure='stale timeout', got %q", taskRef.LastFailure)
	}
	if taskRef.LastFailedAt.IsZero() {
		t.Fatal("expected LastFailedAt to be set, got zero time")
	}
	if taskRef.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", taskRef.Attempts)
	}
}

func TestScheduler_CompleteTask_URIsWithoutChecksums(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	task, _ := scheduler.GetNextTask("w")

	// Provide 2 URIs but nil checksums. This is an asymmetric but supported case
	// for phases that don't produce checksums.
	err := scheduler.CompleteTask("m1", task.GetAttemptID(), []string{"uri1", "uri2"}, nil)
	if err != nil {
		t.Fatalf("expected CompleteTask to succeed with nil checksums, got %v", err)
	}

	outputs := scheduler.GetMapOutputs()
	if len(outputs) != 2 {
		t.Fatalf("expected 2 merged map outputs, got %d", len(outputs))
	}
}

func TestScheduler_MultipleReduceBlockedUntilAllMapComplete(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
		reduceTasks: []Task{
			{ID: "r1", Type: ReduceTask, State: Idle},
			{ID: "r2", Type: ReduceTask, State: Idle},
			{ID: "r3", Type: ReduceTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Pull 2 map tasks
	m1, _ := scheduler.GetNextTask("mw1")
	m2, _ := scheduler.GetNextTask("mw2")

	// Pulling a 3rd time should yield nothing (map busy, reduce waiting)
	_, err := scheduler.GetNextTask("mw3")
	if err != ErrNoIdleTasks {
		t.Fatalf("expected ErrNoIdleTasks before map complete, got %v", err)
	}

	// Complete m1
	_ = scheduler.CompleteTask(m1.ID, m1.GetAttemptID(), nil, nil)

	// Still waiting on m2, reduce tasks should remain blocked
	_, err = scheduler.GetNextTask("rw")
	if err != ErrNoIdleTasks {
		t.Fatalf("expected reduce tasks remaining blocked, got %v", err)
	}

	// Complete m2
	_ = scheduler.CompleteTask(m2.ID, m2.GetAttemptID(), nil, nil)

	// Now reduce tasks should be unblocked
	var reduceTaskIDs []string
	for i := 0; i < 3; i++ {
		rt, err := scheduler.GetNextTask("rw")
		if err != nil {
			t.Fatalf("expected reduce task unblocked, got %v", err)
		}
		if rt.Type != ReduceTask {
			t.Fatalf("expected ReduceTask, got %v", rt.Type)
		}
		reduceTaskIDs = append(reduceTaskIDs, rt.ID)
	}
	if len(reduceTaskIDs) != 3 {
		t.Fatalf("expected 3 reduce tasks, got %d", len(reduceTaskIDs))
	}
}
func TestScheduler_DeepCopy_Isolation(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle, OutputURIs: []string{"s3://old"}},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Get a task snapshot
	task, _ := scheduler.GetTaskByID("m1")

	// Mutate the slice in the copy
	task.OutputURIs[0] = "s3://MUTATED"
	task.OutputURIs = append(task.OutputURIs, "s3://NEW")

	// Internal state must be unchanged
	ref := scheduler.taskMap["m1"]
	if ref.OutputURIs[0] != "s3://old" || len(ref.OutputURIs) != 1 {
		t.Fatalf("internal state was corrupted by external mutation: %v", ref.OutputURIs)
	}
}

func TestScheduler_JobFailure_ImmediatePropagation(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Manually force a task into Failed state
	scheduler.mu.Lock()
	scheduler.taskMap["m1"].State = Failed
	scheduler.mu.Unlock()

	// Now GetNextTask should immediately return ErrJobFailed
	_, err := scheduler.GetNextTask("w2")
	if err != ErrJobFailed {
		t.Fatalf("expected ErrJobFailed, got %v", err)
	}
}

func TestScheduler_QueueOrder(t *testing.T) {
	tracker := &TaskTracker{
		mapTasks: []Task{
			{ID: "m1", Type: MapTask, State: Idle},
			{ID: "m2", Type: MapTask, State: Idle},
			{ID: "m3", Type: MapTask, State: Idle},
		},
	}
	scheduler, _ := NewScheduler(tracker)

	// Should follow FIFO order of the queue
	t1, _ := scheduler.GetNextTask("w")
	if t1.ID != "m1" {
		t.Errorf("expected m1, got %s", t1.ID)
	}

	t2, _ := scheduler.GetNextTask("w")
	if t2.ID != "m2" {
		t.Errorf("expected m2, got %s", t2.ID)
	}

	// Fail t1 -> it should go to the BACK of the queue
	_ = scheduler.FailTask("m1", t1.GetAttemptID(), t1.GetLeaseID(), "crash")

	t3, _ := scheduler.GetNextTask("w")
	if t3.ID != "m3" {
		t.Errorf("expected m3, got %s", t3.ID)
	}

	t4, _ := scheduler.GetNextTask("w")
	if t4.ID != "m1" {
		t.Errorf("expected m1 (reassigned), got %s", t4.ID)
	}
}

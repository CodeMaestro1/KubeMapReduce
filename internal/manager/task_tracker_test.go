package manager

import (
	"testing"
	"time"
)

func TestNewTaskTracker_DefensiveCopy(t *testing.T) {
	mapSrc := []Task{
		{ID: "m1", Type: MapTask, State: Idle},
	}
	reduceSrc := []Task{
		{ID: "r1", Type: ReduceTask, State: Idle},
	}

	tracker := NewTaskTracker(mapSrc, reduceSrc)

	// Mutate the original slices after construction
	mapSrc[0].ID = "MUTATED"
	reduceSrc[0].ID = "MUTATED"

	if tracker.mapTasks[0].ID != "m1" {
		t.Fatalf("expected tracker to hold 'm1', got %q — defensive copy failed", tracker.mapTasks[0].ID)
	}
	if tracker.reduceTasks[0].ID != "r1" {
		t.Fatalf("expected tracker to hold 'r1', got %q — defensive copy failed", tracker.reduceTasks[0].ID)
	}
}

func TestNewTaskTracker_NilSlices(t *testing.T) {
	tracker := NewTaskTracker(nil, nil)
	if tracker.MapTaskCount() != 0 || tracker.ReduceTaskCount() != 0 {
		t.Fatalf("expected 0/0 counts for nil slices, got %d/%d",
			tracker.MapTaskCount(), tracker.ReduceTaskCount())
	}
}

func TestTaskTracker_Counts(t *testing.T) {
	tracker := NewTaskTracker(
		[]Task{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}},
		[]Task{{ID: "r1"}, {ID: "r2"}},
	)

	if tracker.MapTaskCount() != 3 {
		t.Fatalf("expected 3 map tasks, got %d", tracker.MapTaskCount())
	}
	if tracker.ReduceTaskCount() != 2 {
		t.Fatalf("expected 2 reduce tasks, got %d", tracker.ReduceTaskCount())
	}
}

func TestTaskState_String(t *testing.T) {
	tests := []struct {
		state TaskState
		want  string
	}{
		{Idle, "Idle"},
		{InProgress, "InProgress"},
		{Completed, "Completed"},
		{Failed, "Failed"},
		{TaskState(99), "Unknown"},
	}
	for _, tc := range tests {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("TaskState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestTaskType_String(t *testing.T) {
	tests := []struct {
		typ  TaskType
		want string
	}{
		{MapTask, "MapTask"},
		{ReduceTask, "ReduceTask"},
		{TaskType(99), "Unknown"},
	}
	for _, tc := range tests {
		if got := tc.typ.String(); got != tc.want {
			t.Errorf("TaskType(%d).String() = %q, want %q", tc.typ, got, tc.want)
		}
	}
}

func TestJobState_String(t *testing.T) {
	tests := []struct {
		state JobState
		want  string
	}{
		{JobPending, "Pending"},
		{JobRunning, "Running"},
		{JobCleaning, "Cleaning"},
		{JobCompleted, "Completed"},
		{JobFailed, "Failed"},
		{JobCancelled, "Cancelled"},
		{JobState(99), "Unknown"},
	}
	for _, tc := range tests {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("JobState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestTask_Accessors(t *testing.T) {
	now := time.Now()
	task := Task{
		ID:              "t1",
		Type:            MapTask,
		State:           InProgress,
		workerID:        "worker-99",
		startTime:       now,
		ActiveAttemptID: "attempt-abc",
		LeaseID:         "lease-xyz",
		LastHeartbeat:   now,
	}

	if task.WorkerID() != "worker-99" {
		t.Errorf("WorkerID() = %q, want %q", task.WorkerID(), "worker-99")
	}
	if !task.StartTime().Equal(now) {
		t.Errorf("StartTime() = %v, want %v", task.StartTime(), now)
	}
	if task.GetAttemptID() != "attempt-abc" {
		t.Errorf("GetAttemptID() = %q, want %q", task.GetAttemptID(), "attempt-abc")
	}
	if task.GetLeaseID() != "lease-xyz" {
		t.Errorf("GetLeaseID() = %q, want %q", task.GetLeaseID(), "lease-xyz")
	}
	if !task.GetHeartbeat().Equal(now) {
		t.Errorf("GetHeartbeat() = %v, want %v", task.GetHeartbeat(), now)
	}
}

func TestJobRecord_Fields(t *testing.T) {
	tracker := NewTaskTracker(
		[]Task{{ID: "m1", Type: MapTask, State: Idle}},
		[]Task{{ID: "r1", Type: ReduceTask, State: Idle}},
	)

	record := JobRecord{
		JobID:            "job-123",
		State:            JobPending,
		TotalMapTasks:    tracker.MapTaskCount(),
		TotalReduceTasks: tracker.ReduceTaskCount(),
		Tracker:          tracker,
	}

	if record.JobID != "job-123" {
		t.Errorf("expected JobID 'job-123', got %q", record.JobID)
	}
	if record.State != JobPending {
		t.Errorf("expected JobPending, got %v", record.State)
	}
	if record.TotalMapTasks != 1 {
		t.Errorf("expected 1 map task, got %d", record.TotalMapTasks)
	}
	if record.TotalReduceTasks != 1 {
		t.Errorf("expected 1 reduce task, got %d", record.TotalReduceTasks)
	}
}

func TestTask_NewFields(t *testing.T) {
	src := []Task{
		{
			ID:            "m1",
			Type:          MapTask,
			State:         Idle,
			CombinerURI:   "s3://code/combiner.py",
			ReplicaIndex:  3,
			TotalReducers: 10,
		},
	}

	tracker := NewTaskTracker(src, nil)

	// Mutate source to confirm defensive copy
	src[0].CombinerURI = "MUTATED"
	src[0].ReplicaIndex = 99
	src[0].TotalReducers = 99

	task := tracker.mapTasks[0]
	if task.CombinerURI != "s3://code/combiner.py" {
		t.Fatalf("expected CombinerURI 's3://code/combiner.py', got %q", task.CombinerURI)
	}
	if task.ReplicaIndex != 3 {
		t.Fatalf("expected ReplicaIndex 3, got %d", task.ReplicaIndex)
	}
	if task.TotalReducers != 10 {
		t.Fatalf("expected TotalReducers 10, got %d", task.TotalReducers)
	}
}

package manager

import "time"

// TaskState defines the current status of a Map or Reduce task
//
// State transitions:
//   - Idle -> InProgress (via GetNextTask)
//   - InProgress -> Completed (via CompleteTask)
//   - InProgress -> Idle (via FailTask for retries)
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// TaskType defines whether a task is a Map or Reduce task.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	default:
		return "Unknown"
	}
}

// Task represents the metadata the Master needs to track
type TaskInputSplit struct {
	InputURI      string
	ByteStart     int64
	ByteEnd       int64
	SplitChecksum string
}

// Task represents the metadata the Master needs to track
type Task struct {
	ID              string
	JobID           string
	Type            TaskType // MapTask or ReduceTask
	State           TaskState
	workerID        string
	InputFile       string
	ByteStart       int64
	ByteEnd         int64
	PartitionIndex  int
	CodeURI         string
	CombinerURI     string // Optional combiner code location for local pre-aggregation
	InputChecksum   string
	InputSplits     []TaskInputSplit
	ReplicaIndex    int // Manager StatefulSet replica binding (DDS TASKS.replica_index)
	TotalReducers   int // Number of reduce partitions (R) — map workers need this for hash(k) % R
	startTime       time.Time
	ActiveAttemptID string
	LeaseID         string
	LastHeartbeat   time.Time
	OutputURIs      []string
	OutputChecksums []string
	Attempts        int
	LastFailure     string
	LastFailedAt    time.Time
}

// TaskTracker tracks the overall job progress by storing the current
// map and reduce task metadata for a job.
type TaskTracker struct {
	mapTasks    []Task
	reduceTasks []Task
}

func (t *Task) WorkerID() string        { return t.workerID }
func (t *Task) StartTime() time.Time    { return t.startTime }
func (t *Task) GetAttemptID() string    { return t.ActiveAttemptID }
func (t *Task) GetLeaseID() string      { return t.LeaseID }
func (t *Task) GetHeartbeat() time.Time { return t.LastHeartbeat }

// Clone returns a deep copy of the Task metadata.
// This ensures that internal slices (OutputURIs and OutputChecksums) are
// copied to new underlying arrays, preventing external mutation.
func (t *Task) Clone() Task {
	clone := *t
	if t.InputSplits != nil {
		clone.InputSplits = make([]TaskInputSplit, len(t.InputSplits))
		copy(clone.InputSplits, t.InputSplits)
	}
	if t.OutputURIs != nil {
		clone.OutputURIs = make([]string, len(t.OutputURIs))
		copy(clone.OutputURIs, t.OutputURIs)
	}
	if t.OutputChecksums != nil {
		clone.OutputChecksums = make([]string, len(t.OutputChecksums))
		copy(clone.OutputChecksums, t.OutputChecksums)
	}
	return clone
}

// NewTaskTracker safely initializes a TaskTracker by maintaining defensive copies.
func NewTaskTracker(mapTasks, reduceTasks []Task) *TaskTracker {
	mCopy := make([]Task, len(mapTasks))
	for i := range mapTasks {
		mCopy[i] = mapTasks[i].Clone()
	}
	rCopy := make([]Task, len(reduceTasks))
	for i := range reduceTasks {
		rCopy[i] = reduceTasks[i].Clone()
	}
	return &TaskTracker{
		mapTasks:    mCopy,
		reduceTasks: rCopy,
	}
}

// MapTaskCount returns the number of map tasks tracked.
func (tt *TaskTracker) MapTaskCount() int { return len(tt.mapTasks) }

// ReduceTaskCount returns the number of reduce tasks tracked.
func (tt *TaskTracker) ReduceTaskCount() int { return len(tt.reduceTasks) }

// JobState defines the current lifecycle state of a full MapReduce job.
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobCleaning
	JobCompleted
	JobFailed
	JobCancelled
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "Pending"
	case JobRunning:
		return "Running"
	case JobCleaning:
		return "Cleaning"
	case JobCompleted:
		return "Completed"
	case JobFailed:
		return "Failed"
	case JobCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

// JobRecord represents the overarching tracking record for a MapReduce execution.
type JobRecord struct {
	JobID            string
	State            JobState
	TotalMapTasks    int
	TotalReduceTasks int
	Tracker          *TaskTracker
}

package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

// Thresholds for task assignment message size management.
// When a TaskAssignment exceeds the per-server threshold, the server
// uploads the data_locations array to MinIO and returns a manifest URI instead.
// If the manifest payload itself exceeds maxManifestPayloadSizeBytes, the request fails.
//
// maxTaskAssignmentSizeBytes is retained as the default threshold used when
// callers pass <= 0 to NewWorkerServer (e.g. older tests).
var (
	maxTaskAssignmentSizeBytes  = 2 * 1024 * 1024 // 2 MB
	maxManifestPayloadSizeBytes = 2 * 1024 * 1024 // 2 MB
	manifestBucketName          = "mapreduce-manifests"
)

// It acts as the primary communication bridge between the central Manager and
// the distributed Worker pods. It delegates all state transitions and business
// logic to the [manager.Scheduler].
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	scheduler              *manager.Scheduler
	minioClient            *minio.Client
	uploader               manifestUploader
	manifestThresholdBytes int
}

// NewWorkerServer creates a new instance of the gRPC server.
//
// If a [minio.Client] is provided, the server enables "manifest fallback" for
// oversized [pb.TaskAssignment] messages. This is necessary because gRPC has a
// default message size limit (typically 4MB), and a task with thousands of
// input splits could exceed this.
//
// manifestThresholdBytes overrides the default 2 MiB ceiling at which
// assignments are uploaded as manifests. Pass <= 0 to keep the default.
func NewWorkerServer(scheduler *manager.Scheduler, minioClient *minio.Client, manifestThresholdBytes int) *WorkerServer {
	var uploader manifestUploader
	if minioClient != nil {
		uploader = &minioManifestUploader{client: minioClient}
	}
	return newWorkerServerWithManifestUploader(scheduler, minioClient, uploader, manifestThresholdBytes)
}

// Register is called by a Worker immediately after startup to claim its assignment.
//
// The Manager validates that the [pb.RegisterRequest.AttemptId] matches the
// current attempt in the DDS. If it doesn't, the request is rejected as a
// "zombie" worker.
func (s *WorkerServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.TaskAssignment, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	if req.AttemptId == "" {
		return nil, status.Error(codes.InvalidArgument, "attempt_id is required")
	}

	task, err := s.scheduler.GetTaskByID(ctx, req.TaskId)
	if err != nil {
		if errors.Is(err, manager.ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.TaskId)
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	if task.GetAttemptID() != req.AttemptId {
		log.Printf("Register rejected for task %s due to attempt mismatch", req.TaskId)
		return nil, status.Error(codes.PermissionDenied, "attempt rejected")
	}

	assignment := &pb.TaskAssignment{
		TaskId:           task.ID,
		AttemptId:        task.ActiveAttemptID,
		JobId:            task.JobID,
		CodeLocation:     task.CodeURI,
		CombinerLocation: task.CombinerURI,
		RuntimeEnv:       runtimeEnvFromCodeURI(task.CodeURI),
		ByteStart:        task.ByteStart,
		ByteEnd:          task.ByteEnd,
		PartitionId:      0,
		TotalReducers:    int32(task.TotalReducers),
		SplitChecksum:    "",
		LeaseId:          task.LeaseID,
	}

	switch task.Type {
	case manager.MapTask:
		assignment.Type = pb.TaskType_MAP
		if selectedSplit, found := findMapSplitForTask(task); found {
			assignment.ByteStart = selectedSplit.ByteStart
			assignment.ByteEnd = selectedSplit.ByteEnd
			assignment.SplitChecksum = selectedSplit.SplitChecksum
			assignment.DataLocations = append(assignment.DataLocations, selectedSplit.InputURI)
		}
	case manager.ReduceTask:
		assignment.Type = pb.TaskType_REDUCE
		assignment.PartitionId = int32(task.ReducePartition)
		for _, input := range task.ShuffleInputs {
			assignment.DataLocations = append(assignment.DataLocations, input.OutputURI)
		}
	}

	if proto.Size(assignment) > s.manifestThresholdBytes {
		if s.uploader == nil {
			return nil, status.Errorf(codes.ResourceExhausted, "task assignment for task %s exceeds manifest threshold", task.ID)
		}
		manifestBytes, err := json.Marshal(map[string][]string{
			"data_locations": assignment.DataLocations,
		})
		if err != nil {
			log.Printf("Failed to marshal manifest for task %s: %v", task.ID, err)
			return nil, status.Errorf(codes.Internal, "failed to marshal manifest: %v", err)
		}
		if len(manifestBytes) > maxManifestPayloadSizeBytes {
			return nil, status.Errorf(codes.ResourceExhausted, "manifest for task %s exceeds manifest threshold", task.ID)
		}

		objectName := fmt.Sprintf("%s/%s-manifest.json", task.JobID, task.ActiveAttemptID)
		manifestURL, err := s.uploader.UploadManifest(ctx, manifestBucketName, objectName, manifestBytes)
		if err != nil {
			log.Printf("Failed to upload manifest for task %s: %v", task.ID, err)
			return nil, status.Errorf(codes.Unavailable, "failed to upload manifest: %v", err)
		}
		// Embed the SHA-256 digest of the uploaded payload as a URI fragment so the
		// worker can validate manifest integrity without an additional gRPC field.
		// Format: <uri>#sha256=<hex>
		digest := sha256.Sum256(manifestBytes)
		assignment.IsManifest = true
		assignment.DataLocations = []string{fmt.Sprintf("%s#sha256=%s", manifestURL, hex.EncodeToString(digest[:]))}
	} else {
		assignment.IsManifest = false
	}

	if proto.Size(assignment) > s.manifestThresholdBytes {
		// Defensive guard: after manifest replacement this should normally be below threshold,
		// but keep the check for unexpectedly large metadata fields.
		log.Printf("TaskAssignment for task %s still exceeds manifest threshold after manifest fallback", task.ID)
		return nil, status.Errorf(codes.ResourceExhausted, "task assignment for task %s exceeds manifest threshold", task.ID)
	}

	return assignment, nil
}

// Heartbeat is called periodically by Workers to renew their lease on a task.
//
// If the heartbeat fails (e.g. [manager.ErrStaleAttempt]), the response instructs
// the worker to [pb.HeartbeatResponse_TERMINATE] immediately, preventing further
// wasted computation.
func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.RenewLease(ctx, req.TaskId, req.AttemptId, req.LeaseId)
	if err != nil {
		if errors.Is(err, manager.ErrTaskNotFound) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
		}
		if errors.Is(err, manager.ErrStaleAttempt) || errors.Is(err, manager.ErrExpiredLease) || errors.Is(err, manager.ErrInvalidStateTransition) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to renew lease: %v", err)
	}

	return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
}

// TaskComplete is called by a Worker after it has successfully finished its work
// and uploaded results to shared storage.
//
// This call triggers a transactional commit in the DDS, updating the task status
// to "Completed" and recording the output URIs.
func (s *WorkerServer) TaskComplete(ctx context.Context, req *pb.TaskCompleteRequest) (*pb.Ack, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.CompleteTask(ctx, req.TaskId, req.AttemptId, req.LeaseId, req.OutputLocations, req.OutputChecksums)
	if err != nil {
		if errors.Is(err, manager.ErrStaleAttempt) || errors.Is(err, manager.ErrExpiredLease) || errors.Is(err, manager.ErrInvalidStateTransition) {
			return nil, status.Errorf(codes.PermissionDenied, "commit rejected: %v", err)
		}
		if errors.Is(err, manager.ErrOutputMismatch) {
			return nil, status.Errorf(codes.InvalidArgument, "output mismatch: %v", err)
		}
		if errors.Is(err, manager.ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.TaskId)
		}
		return nil, status.Errorf(codes.Internal, "failed to complete task: %v", err)
	}

	return &pb.Ack{Success: true}, nil
}

// TaskFailed is called by a Worker if it encounters an unrecoverable error
// during execution.
//
// The Manager records the error message and resets the task to "Idle" (if
// retries are available) or "Failed" (if the limit is reached).
func (s *WorkerServer) TaskFailed(ctx context.Context, req *pb.TaskFailedRequest) (*pb.Ack, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.FailTask(ctx, req.TaskId, req.AttemptId, req.LeaseId, req.ErrorMessage)
	if err != nil {
		if errors.Is(err, manager.ErrStaleAttempt) || errors.Is(err, manager.ErrExpiredLease) || errors.Is(err, manager.ErrInvalidStateTransition) {
			return nil, status.Errorf(codes.PermissionDenied, "fail task rejected: %v", err)
		}
		if errors.Is(err, manager.ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.TaskId)
		}
		return nil, status.Errorf(codes.Internal, "failed to fail task: %v", err)
	}

	return &pb.Ack{Success: true}, nil
}

// ── Helpers ────────────────────────────────────────────────────────────────

// manifestUploader defines the interface for uploading manifest payloads to shared storage.
type manifestUploader interface {
	UploadManifest(ctx context.Context, bucketName, objectName string, payload []byte) (string, error)
}

// minioManifestUploader implements manifestUploader using a MinIO client.
type minioManifestUploader struct {
	client *minio.Client
}

// UploadManifest uploads a manifest JSON payload to MinIO.
func (m *minioManifestUploader) UploadManifest(ctx context.Context, bucketName, objectName string, payload []byte) (string, error) {
	info, err := m.client.PutObject(ctx, bucketName, objectName, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to upload manifest: %w", err)
	}
	return fmt.Sprintf("s3://%s/%s", info.Bucket, info.Key), nil
}

// newWorkerServerWithManifestUploader creates a WorkerServer with an explicit manifest uploader.
// This is primarily used for testing with mock uploaders.
func newWorkerServerWithManifestUploader(scheduler *manager.Scheduler, minioClient *minio.Client, uploader manifestUploader, manifestThresholdBytes int) *WorkerServer {
	if manifestThresholdBytes <= 0 {
		manifestThresholdBytes = maxTaskAssignmentSizeBytes
	}
	return &WorkerServer{
		scheduler:              scheduler,
		minioClient:            minioClient,
		uploader:               uploader,
		manifestThresholdBytes: manifestThresholdBytes,
	}
}

// splitInfo holds the URI, byte range, and checksum for a single task input split.
type splitInfo struct {
	InputURI      string
	ByteStart     int64
	ByteEnd       int64
	SplitChecksum string
}

// runtimeEnvFromCodeURI infers the worker runtime from the file extension of the
// code artifact URI (e.g. "s3://bucket/mapper.py" → "python").
func runtimeEnvFromCodeURI(codeURI string) string {
	switch strings.ToLower(path.Ext(codeURI)) {
	case ".py":
		return "python"
	case ".jar":
		return "java"
	case ".c":
		return "c"
	case ".cpp", ".cc", ".cxx":
		return "cpp"
	default:
		return ""
	}
}

// findMapSplitForTask returns the single input split assigned to a map task.
// Per the architecture, each map task owns exactly one input split.
func findMapSplitForTask(task *manager.Task) (*splitInfo, bool) {
	if len(task.InputSplits) == 0 {
		return nil, false
	}
	split := task.InputSplits[0]
	return &splitInfo{
		InputURI:      split.InputURI,
		ByteStart:     split.ByteStart,
		ByteEnd:       split.ByteEnd,
		SplitChecksum: split.SplitChecksum,
	}, true
}

package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	scheduler   *manager.Scheduler
	minioClient *minio.Client
	uploader    manifestUploader
}

// manifestBucketName stores serialized data_locations manifests for oversized assignments.
// These objects are short-lived retry metadata and should be managed via bucket lifecycle policy.
const manifestBucketName = "mapreduce-manifests"

var maxTaskAssignmentSizeBytes = 2 * 1024 * 1024

type manifestUploader interface {
	UploadManifest(ctx context.Context, bucketName, objectName string, payload []byte) (string, error)
}

type minioManifestUploader struct {
	client *minio.Client
}

func (m *minioManifestUploader) UploadManifest(ctx context.Context, bucketName, objectName string, payload []byte) (string, error) {
	exists, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return "", err
	}
	if !exists {
		if err := m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			exists, checkErr := m.client.BucketExists(ctx, bucketName)
			if checkErr != nil || !exists {
				return "", err
			}
		}
	}

	_, err = m.client.PutObject(ctx, bucketName, objectName, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("s3://%s/%s", bucketName, objectName), nil
}

func NewWorkerServer(scheduler *manager.Scheduler, minioClient *minio.Client) *WorkerServer {
	var uploader manifestUploader
	if minioClient != nil {
		uploader = &minioManifestUploader{client: minioClient}
	}
	return newWorkerServerWithManifestUploader(scheduler, minioClient, uploader)
}

func newWorkerServerWithManifestUploader(scheduler *manager.Scheduler, minioClient *minio.Client, uploader manifestUploader) *WorkerServer {
	return &WorkerServer{
		scheduler:   scheduler,
		minioClient: minioClient,
		uploader:    uploader,
	}
}

func findMapSplitForTask(task *manager.Task) (manager.TaskInputSplit, bool) {
	if len(task.InputSplits) == 0 {
		return manager.TaskInputSplit{}, false
	}

	for _, split := range task.InputSplits {
		if split.ByteStart == task.ByteStart && split.ByteEnd == task.ByteEnd {
			return split, true
		}
	}

	if task.ByteStart == 0 && task.ByteEnd == 0 && len(task.InputSplits) == 1 {
		return task.InputSplits[0], true
	}

	return manager.TaskInputSplit{}, false
}

func (s *WorkerServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.TaskAssignment, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	if req.AttemptId == "" {
		return nil, status.Error(codes.InvalidArgument, "attempt_id is required")
	}

	task, err := s.scheduler.GetTaskByID(req.TaskId)
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
		RuntimeEnv:       "",
		ByteStart:        task.ByteStart,
		ByteEnd:          task.ByteEnd,
		PartitionId:      0,
		TotalReducers:    int32(task.TotalReducers),
		SplitChecksum:    "",
		LeaseId:          task.LeaseID,
	}

	if task.Type == manager.MapTask {
		assignment.Type = pb.TaskType_MAP
		if selectedSplit, found := findMapSplitForTask(task); found {
			assignment.ByteStart = selectedSplit.ByteStart
			assignment.ByteEnd = selectedSplit.ByteEnd
			assignment.SplitChecksum = selectedSplit.SplitChecksum
		}
		for _, split := range task.InputSplits {
			assignment.DataLocations = append(assignment.DataLocations, split.InputURI)
		}
	} else if task.Type == manager.ReduceTask {
		assignment.Type = pb.TaskType_REDUCE
		assignment.PartitionId = int32(task.ReducePartition)
		for _, input := range task.ShuffleInputs {
			assignment.DataLocations = append(assignment.DataLocations, input.OutputURI)
		}
	}

	if proto.Size(assignment) > maxTaskAssignmentSizeBytes {
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

		objectName := fmt.Sprintf("%s/%s-manifest.json", task.JobID, task.ActiveAttemptID)
		manifestURL, err := s.uploader.UploadManifest(ctx, manifestBucketName, objectName, manifestBytes)
		if err != nil {
			log.Printf("Failed to upload manifest for task %s: %v", task.ID, err)
			return nil, status.Errorf(codes.Unavailable, "failed to upload manifest: %v", err)
		}
		assignment.IsManifest = true
		assignment.DataLocations = []string{manifestURL}
	} else {
		assignment.IsManifest = false
	}

	if proto.Size(assignment) > maxTaskAssignmentSizeBytes {
		// Defensive guard: after manifest replacement this should normally be below threshold,
		// but keep the check for unexpectedly large metadata fields.
		log.Printf("TaskAssignment for task %s still exceeds manifest threshold after manifest fallback", task.ID)
		return nil, status.Errorf(codes.ResourceExhausted, "task assignment for task %s exceeds manifest threshold", task.ID)
	}

	return assignment, nil
}

func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.RenewLease(req.TaskId, req.AttemptId, req.LeaseId)
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

func (s *WorkerServer) TaskComplete(ctx context.Context, req *pb.TaskCompleteRequest) (*pb.Ack, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.CompleteTask(req.TaskId, req.AttemptId, req.LeaseId, req.OutputLocations, req.OutputChecksums)
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

func (s *WorkerServer) TaskFailed(ctx context.Context, req *pb.TaskFailedRequest) (*pb.Ack, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	err := s.scheduler.FailTask(req.TaskId, req.AttemptId, req.LeaseId, req.ErrorMessage)
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

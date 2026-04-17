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

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	scheduler   *manager.Scheduler
	minioClient *minio.Client
}

func NewWorkerServer(scheduler *manager.Scheduler, minioClient *minio.Client) *WorkerServer {
	return &WorkerServer{
		scheduler:   scheduler,
		minioClient: minioClient,
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
		return nil, status.Errorf(codes.PermissionDenied, "attempt_id mismatch: got %s, expected %s", req.AttemptId, task.GetAttemptID())
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
		assignment.PartitionId = int32(task.ReplicaIndex)
		for _, input := range task.ShuffleInputs {
			assignment.DataLocations = append(assignment.DataLocations, input.OutputURI)
		}
	}

	if len(assignment.DataLocations) > 1000 && s.minioClient != nil {
		manifestBytes, err := json.Marshal(assignment.DataLocations)
		if err != nil {
			log.Printf("Failed to marshal manifest for task %s: %v", task.ID, err)
		} else {
			bucketName := "mapreduce-manifests"
			objectName := fmt.Sprintf("%s-manifest.json", task.ID)

			// Best-effort bucket creation
			exists, _ := s.minioClient.BucketExists(ctx, bucketName)
			if !exists {
				_ = s.minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
			}

			_, err = s.minioClient.PutObject(ctx, bucketName, objectName, bytes.NewReader(manifestBytes), int64(len(manifestBytes)), minio.PutObjectOptions{
				ContentType: "application/json",
			})

			if err != nil {
				log.Printf("Failed to upload manifest for task %s: %v", task.ID, err)
				assignment.IsManifest = false
			} else {
				assignment.IsManifest = true
				manifestURL := fmt.Sprintf("s3://%s/%s", bucketName, objectName)
				assignment.DataLocations = []string{manifestURL}
			}
		}
	} else {
		assignment.IsManifest = false
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

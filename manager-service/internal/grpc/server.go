package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	scheduler *manager.Scheduler
}

func NewWorkerServer(scheduler *manager.Scheduler) *WorkerServer {
	return &WorkerServer{
		scheduler: scheduler,
	}
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
		if len(task.InputSplits) > 0 {
			selectedSplit := task.InputSplits[0]
			for _, split := range task.InputSplits {
				if split.ByteStart == task.ByteStart && split.ByteEnd == task.ByteEnd {
					selectedSplit = split
					break
				}
			}
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

	// TODO: Issue #56 "Missing manifest fallback (is_manifest) for large reduce metadata payloads"
	// For now we don't implement the manifest, but we leave the boolean field
	assignment.IsManifest = false

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

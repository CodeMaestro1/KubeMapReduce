package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/config"
)

// Worker executes a single MapReduce task on behalf of the Manager.
type Worker struct {
	cfg     *config.Config
	client  pb.WorkerServiceClient
	storage objectStorage

	// prepareCode downloads user code and returns (execPath, cleanup, err).
	// Swappable in tests to avoid real MinIO and compiler calls.
	prepareCode func(ctx context.Context, s objectStorage, codeURI, tempDir string) (string, func(), error)

	// execCode runs the user binary with JSONL on stdin and returns stdout.
	// Swappable in tests to avoid real subprocess execution.
	execCode func(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) ([]byte, error)
}

// New creates a production Worker wired to the given gRPC client and MinIO instance.
func New(cfg *config.Config, client pb.WorkerServiceClient, minioClient *minio.Client) *Worker {
	var s objectStorage
	if minioClient != nil {
		s = newMinioStorage(minioClient)
	}
	return &Worker{
		cfg:         cfg,
		client:      client,
		storage:     s,
		prepareCode: downloadCode,
		execCode:    runUserCode,
	}
}

// Run is the top-level entry point. It registers with the Manager, executes
// the assigned task, and reports the result. Cancelling ctx (e.g. SIGTERM)
// triggers a TaskFailed RPC before returning.
func (w *Worker) Run(ctx context.Context) error {
	assignment, err := w.client.Register(ctx, &pb.RegisterRequest{
		TaskId:    w.cfg.TaskID,
		AttemptId: w.cfg.AttemptID,
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	log.Printf("[worker] registered task=%s attempt=%s type=%s", assignment.TaskId, assignment.AttemptId, assignment.Type)

	// Resolve manifest if is_manifest=true: DataLocations[0] is an s3:// URI.
	if assignment.IsManifest {
		locs, fetchErr := fetchManifest(ctx, w.storage, assignment.DataLocations[0])
		if fetchErr != nil {
			_ = w.reportFailure(context.Background(), assignment, fmt.Sprintf("manifest fetch: %v", fetchErr))
			return fetchErr
		}
		assignment.DataLocations = locs
	}

	// Derive a task context that the heartbeat goroutine can cancel on TERMINATE/SIGTERM.
	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()

	// terminated receives the reason string when the task must abort.
	terminated := make(chan string, 1)
	go w.heartbeatLoop(ctx, assignment, taskCancel, terminated)

	var outputURIs, outputChecksums []string
	switch assignment.Type {
	case pb.TaskType_MAP:
		outputURIs, outputChecksums, err = w.runMap(taskCtx, assignment)
	case pb.TaskType_REDUCE:
		outputURIs, outputChecksums, err = w.runReduce(taskCtx, assignment)
	default:
		err = fmt.Errorf("unknown task type: %v", assignment.Type)
	}

	// Use a fresh context for the final RPC (taskCtx may already be cancelled).
	reportCtx := context.Background()

	// If the heartbeat loop sent a termination signal, report failure and exit.
	select {
	case reason := <-terminated:
		_ = w.reportFailure(reportCtx, assignment, "terminated: "+reason)
		return fmt.Errorf("terminated: %s", reason)
	default:
	}

	if err != nil {
		_ = w.reportFailure(reportCtx, assignment, err.Error())
		return err
	}

	_, rpcErr := w.client.TaskComplete(reportCtx, &pb.TaskCompleteRequest{
		TaskId:          assignment.TaskId,
		AttemptId:       assignment.AttemptId,
		LeaseId:         assignment.LeaseId,
		OutputLocations: outputURIs,
		OutputChecksums: outputChecksums,
	})
	return rpcErr
}

// heartbeatLoop sends periodic Heartbeat RPCs.
// On TERMINATE response or context cancellation it cancels taskCancel and
// sends the reason to terminated (buffered, so it never blocks).
func (w *Worker) heartbeatLoop(ctx context.Context, a *pb.TaskAssignment, taskCancel context.CancelFunc, terminated chan<- string) {
	ticker := time.NewTicker(time.Duration(w.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Parent context cancelled: SIGTERM or caller shutdown.
			terminated <- "SIGTERM"
			taskCancel()
			return
		case <-ticker.C:
			resp, err := w.client.Heartbeat(ctx, &pb.HeartbeatRequest{
				TaskId:    a.TaskId,
				AttemptId: a.AttemptId,
				LeaseId:   a.LeaseId,
			})
			if err != nil {
				if status.Code(err) != codes.Canceled {
					log.Printf("[worker] heartbeat error: %v", err)
				}
				continue
			}
			if resp.Action == pb.HeartbeatResponse_TERMINATE {
				log.Printf("[worker] manager sent TERMINATE")
				terminated <- "manager TERMINATE"
				taskCancel()
				return
			}
		}
	}
}

// reportFailure calls TaskFailed and logs any resulting error.
func (w *Worker) reportFailure(ctx context.Context, a *pb.TaskAssignment, reason string) error {
	log.Printf("[worker] reporting failure: %s", reason)
	_, err := w.client.TaskFailed(ctx, &pb.TaskFailedRequest{
		TaskId:       a.TaskId,
		AttemptId:    a.AttemptId,
		LeaseId:      a.LeaseId,
		ErrorMessage: reason,
	})
	return err
}

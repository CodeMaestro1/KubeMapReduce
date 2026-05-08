package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/config"
)

// finalizationRPCTimeout bounds terminal RPCs (TaskComplete / TaskFailed) so a
// slow or unreachable Manager cannot block worker shutdown indefinitely.
// See issue #110.
const finalizationRPCTimeout = 5 * time.Second

// Worker executes a single MapReduce task on behalf of the Manager.
type Worker struct {
	cfg           *config.Config
	client        pb.WorkerServiceClient
	shuffleClient pb.ShuffleServiceClient
	storage       objectStorage

	// prepareCode downloads user code and returns (execPath, cleanup, err).
	// Swappable in tests to avoid real MinIO and compiler calls.
	prepareCode func(ctx context.Context, s objectStorage, codeURI, tempDir string) (string, func(), error)

	// execCode runs the user binary with JSONL on stdin and returns stdout.
	// Swappable in tests to avoid real subprocess execution.
	execCode func(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) ([]byte, error)
}

// New creates a production Worker wired to the given gRPC client and MinIO instance.
func New(cfg *config.Config, client pb.WorkerServiceClient, shuffleClient pb.ShuffleServiceClient, minioClient *minio.Client) *Worker {
	var s objectStorage
	if minioClient != nil {
		s = newMinioStorage(minioClient)
	} else {
		s = newUnavailableStorage(fmt.Errorf("object storage is not configured"))
	}
	return &Worker{
		cfg:           cfg,
		client:        client,
		shuffleClient: shuffleClient,
		storage:       s,
		prepareCode:   downloadCode,
		execCode:      runUserCode,
	}
}

// Run establishes a persistent connection to the Manager via TaskStream and
// executes tasks as they are assigned.
func (w *Worker) Run(ctx context.Context) error {
	stream, err := w.client.TaskStream(ctx)
	if err != nil {
		return fmt.Errorf("open task stream: %w", err)
	}

	workerID := fmt.Sprintf("worker-%s-%s", w.cfg.JobID, uuid.NewString()[:8])
	log.Printf("[worker] pool started id=%s job=%s", workerID, w.cfg.JobID)

	for {
		// 1. Signal ready for next task
		err = stream.Send(&pb.StreamRequest{
			WorkerId: workerID,
			Payload: &pb.StreamRequest_Ready{
				Ready: &pb.ReadyForTaskRequest{JobId: w.cfg.JobID},
			},
		})
		if err != nil {
			return fmt.Errorf("send ready: %w", err)
		}

		// 2. Wait for assignment
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive assignment: %w", err)
		}

		assignment := resp.GetAssignment()
		if assignment == nil {
			// Not an assignment (maybe an ACK for a terminal request or error)
			// If Success=false it means no tasks, wait and retry.
			if ack := resp.GetAck(); ack != nil && !ack.Success {
				time.Sleep(5 * time.Second)
				continue
			}
			continue
		}

		log.Printf("[worker] assigned task=%s type=%s", assignment.TaskId, assignment.Type)
		if err := w.executeTask(ctx, stream, workerID, assignment); err != nil {
			log.Printf("[worker] task %s failed: %v", assignment.TaskId, err)
			// Continue to next task
		}
	}
}

func (w *Worker) executeTask(ctx context.Context, stream pb.WorkerService_TaskStreamClient, workerID string, assignment *pb.TaskAssignment) error {
	if w.storage == nil {
		return fmt.Errorf("object storage is not configured")
	}

	// Resolve manifest if needed
	if assignment.IsManifest {
		if len(assignment.DataLocations) == 0 {
			return fmt.Errorf("manifest assignment requires at least one data location URI")
		}
		manifestURI := assignment.DataLocations[0]
		locs, err := fetchManifest(ctx, w.storage, manifestURI)
		if err != nil {
			return fmt.Errorf("fetch manifest: %w", err)
		}
		assignment.DataLocations = locs
	}

	taskCtx, taskCancel := context.WithCancel(ctx)
	sendMu := &sync.Mutex{}

	// terminated receives the reason string when the task must abort.
	terminated := make(chan string, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		w.streamHeartbeatLoop(taskCtx, stream, workerID, assignment, terminated, taskCancel, sendMu)
	}()

	var outputURIs, outputChecksums []string
	var err error
	switch assignment.Type {
	case pb.TaskType_MAP:
		outputURIs, outputChecksums, err = w.runMap(taskCtx, assignment)
	case pb.TaskType_REDUCE:
		outputURIs, outputChecksums, err = w.runReduce(taskCtx, assignment)
	default:
		err = fmt.Errorf("unknown task type: %v", assignment.Type)
	}

	taskCancel()
	<-heartbeatDone

	if err != nil {
		// Report failure via stream
		sendMu.Lock()
		sendErr := stream.Send(&pb.StreamRequest{
			WorkerId: workerID,
			Payload: &pb.StreamRequest_Failed{
				Failed: &pb.TaskFailedRequest{
					TaskId:       assignment.TaskId,
					AttemptId:    assignment.AttemptId,
					LeaseId:      assignment.LeaseId,
					ErrorMessage: err.Error(),
				},
			},
		})
		sendMu.Unlock()
		if sendErr != nil {
			log.Printf("[worker] failed to report TaskFailed via stream: %v", sendErr)
			return fmt.Errorf("task error: %w; also failed to report failure: %v", err, sendErr)
		}
		return err
	}

	// Report success via stream
	sendMu.Lock()
	err = stream.Send(&pb.StreamRequest{
		WorkerId: workerID,
		Payload: &pb.StreamRequest_Complete{
			Complete: &pb.TaskCompleteRequest{
				TaskId:          assignment.TaskId,
				AttemptId:       assignment.AttemptId,
				LeaseId:         assignment.LeaseId,
				OutputLocations: outputURIs,
				OutputChecksums: outputChecksums,
			},
		},
	})
	sendMu.Unlock()
	return err
}

func (w *Worker) streamHeartbeatLoop(
	ctx context.Context,
	stream pb.WorkerService_TaskStreamClient,
	workerID string,
	a *pb.TaskAssignment,
	terminated chan<- string,
	taskCancel context.CancelFunc,
	sendMu *sync.Mutex,
) {
	ticker := time.NewTicker(time.Duration(w.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendMu.Lock()
			err := stream.Send(&pb.StreamRequest{
				WorkerId: workerID,
				Payload: &pb.StreamRequest_Heartbeat{
					Heartbeat: &pb.HeartbeatRequest{
						TaskId:    a.TaskId,
						AttemptId: a.AttemptId,
						LeaseId:   a.LeaseId,
					},
				},
			})
			sendMu.Unlock()
			if err != nil {
				log.Printf("[worker] heartbeat send error: %v", err)
				return
			}
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("[worker] heartbeat recv error: %v", err)
				return
			}
			if hb := resp.GetHeartbeatAck(); hb != nil && hb.Action == pb.HeartbeatResponse_TERMINATE {
				select {
				case terminated <- "manager TERMINATE":
				default:
				}
				taskCancel()
				return
			}
		}
	}
}

// heartbeatLoop sends periodic Heartbeat RPCs.
// On TERMINATE response or context cancellation it cancels taskCancel and
// sends the reason to terminated (buffered, so it never blocks).
func (w *Worker) heartbeatLoop(ctx context.Context, a *pb.TaskAssignment, taskCancel context.CancelFunc, terminated chan<- string, reporter *failureReporter) {
	ticker := time.NewTicker(time.Duration(w.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Parent context cancelled: SIGTERM or caller shutdown.
			reporter.start(w, a, "SIGTERM")
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
				reporter.start(w, a, "manager TERMINATE")
				terminated <- "manager TERMINATE"
				taskCancel()
				return
			}
		}
	}
}

// reportFailure calls TaskFailed and logs any resulting error. If the supplied
// context has no deadline (e.g. context.Background()), a fallback timeout is
// enforced so the RPC cannot hang forever when the Manager is unreachable
// (issue #110).
func (w *Worker) reportFailure(ctx context.Context, a *pb.TaskAssignment, reason string) error {
	log.Printf("[worker] reporting failure: %s", reason)
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, finalizationRPCTimeout)
		defer cancel()
	}
	_, err := w.client.TaskFailed(ctx, &pb.TaskFailedRequest{
		TaskId:       a.TaskId,
		AttemptId:    a.AttemptId,
		LeaseId:      a.LeaseId,
		ErrorMessage: reason,
	})
	return err
}

type failureReporter struct {
	once sync.Once
	wg   sync.WaitGroup
}

func (r *failureReporter) start(w *Worker, a *pb.TaskAssignment, reason string) {
	r.once.Do(func() {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			reportCtx, cancel := context.WithTimeout(context.Background(), finalizationRPCTimeout)
			defer cancel()
			_ = w.reportFailure(reportCtx, a, reason)
		}()
	})
}

func (r *failureReporter) wait(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func terminationReason(terminated <-chan string, ctx, taskCtx context.Context) (string, bool) {
	select {
	case reason := <-terminated:
		return reason, true
	default:
	}
	if ctx.Err() != nil {
		return "SIGTERM", true
	}
	if taskCtx.Err() != nil {
		return "manager TERMINATE", true
	}
	return "", false
}

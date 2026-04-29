package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/internal/manager"
	"kubemapreduce/manager-service/internal/models"
	"kubemapreduce/manager-service/internal/validation"
	"kubemapreduce/manager-service/pkg/httputil"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

// Handlers holds HTTP handler state for the API server.
type Handlers struct {
	adminClient    *auth.KeycloakAdminClient
	store          JobStore
	minioClient    *minio.Client
	managerAddr    string
	internalAPIKey string
	httpClient     *http.Client
	now            func() time.Time
}

const (
	defaultReducers  = 1
	defaultListLimit = 100
	maxListLimit     = 500
	maxJSONBodyBytes = 1 << 20 // 1 MiB
	inputBucketName  = "inputs"
)

var defaultTargetSplitSizeBytes int64 = 64 * 1024 * 1024

// NewHandlers creates production-ready Handlers.
func NewHandlers(adminClient *auth.KeycloakAdminClient, store JobStore, minioClient *minio.Client, managerAddr string, internalAPIKey string) *Handlers {
	return &Handlers{
		adminClient:    adminClient,
		store:          store,
		minioClient:    minioClient,
		managerAddr:    managerAddr,
		internalAPIKey: internalAPIKey,
		httpClient:     http.DefaultClient,
		now:            time.Now,
	}
}

func newHandlersWithOptions(adminClient *auth.KeycloakAdminClient, store JobStore, minioClient *minio.Client, managerAddr string, internalAPIKey string, now func() time.Time) *Handlers {
	if now == nil {
		now = time.Now
	}
	return &Handlers{
		adminClient:    adminClient,
		store:          store,
		minioClient:    minioClient,
		managerAddr:    managerAddr,
		internalAPIKey: internalAPIKey,
		httpClient:     http.DefaultClient,
		now:            now,
	}
}

// HandleRoot provides a basic discovery endpoint for the API.
//
// It serves as a heartbeat for the CLI and a way to verify the API's base URL.
// Returns 404 for any sub-paths to prevent accidental masking of other routes.
func (h *Handlers) HandleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"name":    "KubeMapReduce API",
		"status":  "running",
		"message": "Use the CLI to interact with this API. See README for details.",
	}); err != nil {
		return
	}
}

// HandleHealth provides an unauthenticated endpoint for monitoring tools.
//
// It returns a simple 200 OK status to indicate the web server is processing
// requests. This is distinct from deep health checks that might probe the
// database or Keycloak.
func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"}); err != nil {
		return
	}
}

// HandleJobsSubmit processes new MapReduce job requests.
//
// This handler validates the job specification and persists it to the [JobStore]
// with a "Pending" status. It implements a metadata-only submission pattern:
// the actual input and code files are expected to be reachable via the provided
// URIs. Returns 202 Accepted, reflecting that the job has been queued for
// scheduling but hasn't necessarily started execution.
func (h *Handlers) HandleJobsSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var request models.JobSubmissionRequest
	if !decodeJSONBody(w, r, &request, "invalid job payload") {
		return
	}

	if request.Reducers == 0 {
		request.Reducers = defaultReducers
	}

	if err := validation.ValidateJobSubmission(request); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	jobID := uuid.New().String()
	now := h.now().UTC()

	combinerURI := ""
	if request.Combiner != nil {
		combinerURI = request.Combiner.Artifact
	}

	schedReq, err := buildScheduleRequest(r.Context(), newScheduleObjectClient(h.minioClient), jobID, userID, request, combinerURI)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("failed to prepare job splits: %v", err))
		return
	}

	rec := JobRecord{
		JobID:         jobID,
		UserID:        userID,
		Status:        "Pending",
		Filename:      request.Filename,
		InputChecksum: request.InputChecksum,
		Reducers:      request.Reducers,
		CreatedAt:     now,
		MapperURI:     request.Mapper.Artifact,
		ReducerURI:    request.Reducer.Artifact,
		CombinerURI:   combinerURI,
		MTasks:        1,
	}
	if err := h.store.CreateJob(r.Context(), rec); err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to persist job")
		return
	}

	if h.managerAddr != "" {
		if err := h.postSchedule(r.Context(), schedReq); err != nil {
			log.Printf("[api] job %s persisted but schedule failed: %v", jobID, err)
		}
	}

	response := models.JobSubmissionResponse{
		JobID:   jobID,
		Status:  "accepted",
		Message: "job specification validated and accepted (metadata only — no file transfer)",
	}

	if err := httputil.WriteJSON(w, http.StatusAccepted, response); err != nil {
		return
	}
}

// HandleJobsList retrieves a paginated list of all MapReduce jobs.
//
// It supports `limit` and `offset` query parameters to allow the CLI to
// efficiently browse large job histories. The output is sorted by creation
// time in descending order to prioritize recent activity.
func (h *Handlers) HandleJobsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit, offset, err := parsePagination(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var records []JobRecord
	if auth.HasRole(r, "ADMIN") {
		records, err = h.store.ListAllJobs(r.Context(), limit, offset)
	} else {
		records, err = h.store.ListJobs(r.Context(), userID, limit, offset)
	}
	if err != nil {
		if errors.Is(err, ErrInvalidUserID) {
			httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: invalid subject")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	list := make([]models.JobStatusResponse, 0, len(records))
	for _, rec := range records {
		list = append(list, models.JobStatusResponse{
			JobID:     rec.JobID,
			Status:    rec.Status,
			Message:   jobMessage(rec.Status),
			Filename:  rec.Filename,
			Reducers:  rec.Reducers,
			CreatedAt: rec.CreatedAt,
		})
	}

	if err := httputil.WriteJSON(w, http.StatusOK, list); err != nil {
		return
	}
}

func parsePagination(r *http.Request) (int, int, error) {
	limit := defaultListLimit
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			return 0, 0, errors.New("limit must be a positive integer")
		}
		if parsedLimit > maxListLimit {
			parsedLimit = maxListLimit
		}
		limit = parsedLimit
	}

	offset := 0
	if rawOffset := strings.TrimSpace(r.URL.Query().Get("offset")); rawOffset != "" {
		parsedOffset, err := strconv.Atoi(rawOffset)
		if err != nil || parsedOffset < 0 {
			return 0, 0, errors.New("offset must be a non-negative integer")
		}
		offset = parsedOffset
	}
	return limit, offset, nil
}

// HandleJobsGet retrieves detailed status for a single job by ID.
//
// This is the primary polling endpoint for CLI status checks. It returns the
// current phase and metadata for the job. Returns 404 if the job_id does
// not exist in the [JobStore].
func (h *Handlers) HandleJobsGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := r.PathValue("job_id")
	if jobID == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "job id required")
		return
	}
	if _, err := uuid.Parse(jobID); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var rec *JobRecord
	if auth.HasRole(r, "ADMIN") {
		rec, err = h.store.GetAnyJob(r.Context(), jobID)
	} else {
		rec, err = h.store.GetJob(r.Context(), userID, jobID)
	}
	if err != nil {
		if errors.Is(err, ErrInvalidJobID) {
			httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
			return
		}
		if errors.Is(err, ErrInvalidUserID) {
			httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: invalid subject")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to retrieve job")
		return
	}
	if rec == nil {
		httputil.WriteErrorJSON(w, http.StatusNotFound, "job not found")
		return
	}

	resp := models.JobStatusResponse{
		JobID:     rec.JobID,
		Status:    rec.Status,
		Message:   jobMessage(rec.Status),
		Filename:  rec.Filename,
		Reducers:  rec.Reducers,
		CreatedAt: rec.CreatedAt,
	}

	if err := httputil.WriteJSON(w, http.StatusOK, resp); err != nil {
		return
	}
}

// HandleJobsDownload provides access to the final results of a completed job.
//
// Note: This endpoint is currently a placeholder and returns 501 Not Implemented,
// as the result aggregation and streaming logic from shared storage is
// pending backend integration.
func (h *Handlers) HandleJobsDownload(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "job id required")
		return
	}
	if _, err := uuid.Parse(jobID); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var rec *JobRecord
	if auth.HasRole(r, "ADMIN") {
		rec, err = h.store.GetAnyJob(r.Context(), jobID)
	} else {
		rec, err = h.store.GetJob(r.Context(), userID, jobID)
	}
	if err != nil {
		if errors.Is(err, ErrInvalidJobID) {
			httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
			return
		}
		if errors.Is(err, ErrInvalidUserID) {
			httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: invalid subject")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to retrieve job")
		return
	}
	if rec == nil {
		httputil.WriteErrorJSON(w, http.StatusNotFound, "job not found")
		return
	}

	if rec.Status != "Completed" {
		httputil.WriteJSON(w, http.StatusConflict, map[string]any{
			"error":  "job_not_complete",
			"status": rec.Status,
		})
		return
	}

	outputURIs, err := h.store.GetJobOutputs(r.Context(), jobID)
	if err != nil {
		log.Printf("GetJobOutputs failed for job %s: %v", jobID, err)
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to retrieve job outputs")
		return
	}

	if h.minioClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "storage not available")
		return
	}

	presigned := make([]string, 0, len(outputURIs))
	for _, uri := range outputURIs {
		bucket, key, parseErr := parseOutputURI(uri)
		if parseErr != nil {
			log.Printf("invalid output URI for job %s: %v", jobID, parseErr)
			httputil.WriteErrorJSON(w, http.StatusInternalServerError, "invalid output URI")
			return
		}
		u, presignErr := h.minioClient.PresignedGetObject(r.Context(), bucket, key, 15*time.Minute, nil)
		if presignErr != nil {
			log.Printf("presign failed for job %s uri %s: %v", jobID, uri, presignErr)
			httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to generate download URL")
			return
		}
		presigned = append(presigned, u.String())
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"jobId": jobID,
		"urls":  presigned,
	})
}

// parseOutputURI splits an s3://bucket/key URI into bucket and key components.
func parseOutputURI(uri string) (bucket, key string, err error) {
	const prefix = "s3://"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", fmt.Errorf("unsupported URI scheme: %q", uri)
	}
	rest := uri[len(prefix):]
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("missing key in URI: %q", uri)
	}
	return rest[:idx], rest[idx+1:], nil
}

// HandleConfigureNodes updates resource limits for the MapReduce cluster nodes.
//
// This is an administrative endpoint used to fine-tune the maximum pod density
// and resource quotas (CPU/Memory) across the compute fleet. It currently
// returns 501 Not Implemented as the integration with the cluster scheduler
// is still in progress.
func (h *Handlers) HandleConfigureNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req models.NodeConfigRequest
	if !decodeJSONBody(w, r, &req, "invalid node config payload") {
		return
	}

	if req.MaxPods < 1 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "maxPods must be a positive integer")
		return
	}
	if strings.TrimSpace(req.CPULimit) == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "cpuLimit is required")
		return
	}
	if strings.TrimSpace(req.MemoryLimit) == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "memoryLimit is required")
		return
	}

	// proxy to manager internal endpoint
	update := manager.SystemConfigUpdate{
		MaxConcurrentPods: req.MaxPods,
		CPULimit:          req.CPULimit,
		MemoryLimit:       req.MemoryLimit,
	}

	payload, err := json.Marshal(update)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to serialize config update")
		return
	}
	managerURL := fmt.Sprintf("http://%s/internal/config", h.managerAddr)
	proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodPut, managerURL, bytes.NewReader(payload))
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to build proxy request")
		return
	}
	if h.internalAPIKey != "" {
		proxyReq.Header.Set("X-Internal-Token", h.internalAPIKey)
	}

	resp, err := h.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "manager service unreachable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "failed to update config via manager", resp.StatusCode)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "success",
		"maxPods":     req.MaxPods,
		"cpuLimit":    req.CPULimit,
		"memoryLimit": req.MemoryLimit,
	}); err != nil {
		return
	}
}

// HandleWorkerConfig updates the global configuration for Worker pods.
//
// Admins use this to control the parallelism of the system (replicas) and
// task-packing density. This endpoint returns 202 Accepted to signal that the
// new configuration has been received and will be applied to future jobs.
func (h *Handlers) HandleWorkerConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var request models.WorkerConfigRequest
	if !decodeJSONBody(w, r, &request, "invalid worker config payload") {
		return
	}

	if request.WorkerReplicas < 1 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "workerReplicas must be positive")
		return
	}
	if request.MaxJobsPerNode < 1 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "maxJobsPerNode must be positive")
		return
	}

	// proxy to manager internal endpoint
	update := manager.SystemConfigUpdate{
		WorkerReplicas: request.WorkerReplicas,
		MaxJobsPerNode: request.MaxJobsPerNode,
	}

	payload, err := json.Marshal(update)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to serialize config update")
		return
	}
	managerURL := fmt.Sprintf("http://%s/internal/config", h.managerAddr)
	proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodPut, managerURL, bytes.NewReader(payload))
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to build proxy request")
		return
	}
	if h.internalAPIKey != "" {
		proxyReq.Header.Set("X-Internal-Token", h.internalAPIKey)
	}

	resp, err := h.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "manager service unreachable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "failed to update config via manager", resp.StatusCode)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "worker configuration update accepted",
		"workerReplicas": request.WorkerReplicas,
		"maxJobsPerNode": request.MaxJobsPerNode,
	}); err != nil {
		return
	}
}

// HandleAdminConfigWorkers is the unified POST /api/v1/admin/config/workers handler.
//
// It replaces the legacy PUT /api/v1/admin/workers/config and PUT /api/v1/admin/nodes/config
// endpoints, consolidating all cluster configuration into a single call per the design spec.
// All fields are optional; callers may supply any subset.
func (h *Handlers) HandleAdminConfigWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req models.AdminWorkerConfigRequest
	if !decodeJSONBody(w, r, &req, "invalid worker config payload") {
		return
	}

	if req.MaxPods < 0 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "maxPods must be non-negative")
		return
	}
	if req.WorkerReplicas < 0 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "workerReplicas must be non-negative")
		return
	}
	if req.MaxJobsPerNode < 0 {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "maxJobsPerNode must be non-negative")
		return
	}
	if req.MaxPods == 0 && req.WorkerReplicas == 0 && req.MaxJobsPerNode == 0 &&
		strings.TrimSpace(req.CPULimit) == "" && strings.TrimSpace(req.MemoryLimit) == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "at least one configuration field must be provided")
		return
	}

	update := manager.SystemConfigUpdate{
		MaxConcurrentPods: req.MaxPods,
		CPULimit:          req.CPULimit,
		MemoryLimit:       req.MemoryLimit,
		WorkerReplicas:    req.WorkerReplicas,
		MaxJobsPerNode:    req.MaxJobsPerNode,
	}

	payload, err := json.Marshal(update)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to serialize config update")
		return
	}
	managerURL := fmt.Sprintf("http://%s/internal/config", h.managerAddr)
	proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodPut, managerURL, bytes.NewReader(payload))
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to build proxy request")
		return
	}
	if h.internalAPIKey != "" {
		proxyReq.Header.Set("X-Internal-Token", h.internalAPIKey)
	}

	resp, err := h.httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "manager service unreachable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "failed to update config via manager", resp.StatusCode)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":         "accepted",
		"maxPods":        req.MaxPods,
		"cpuLimit":       req.CPULimit,
		"memoryLimit":    req.MemoryLimit,
		"workerReplicas": req.WorkerReplicas,
		"maxJobsPerNode": req.MaxJobsPerNode,
	}); err != nil {
		return
	}
}

// postSchedule POSTs a ScheduleJobRequest to the Manager's internal schedule endpoint.
func (h *Handlers) postSchedule(ctx context.Context, req manager.ScheduleJobRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal schedule request: %w", err)
	}
	url := fmt.Sprintf("http://%s/internal/schedule", h.managerAddr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build schedule request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if h.internalAPIKey != "" {
		httpReq.Header.Set("X-Internal-Token", h.internalAPIKey)
	}
	resp, err := h.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("schedule POST: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("schedule POST returned %d", resp.StatusCode)
	}
	return nil
}

type scheduleObjectClient interface {
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
}

type minioScheduleObjectClient struct {
	client *minio.Client
}

func newScheduleObjectClient(client *minio.Client) scheduleObjectClient {
	if client == nil {
		return nil
	}
	return &minioScheduleObjectClient{client: client}
}

func (m *minioScheduleObjectClient) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return m.client.StatObject(ctx, bucketName, objectName, opts)
}

func (m *minioScheduleObjectClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

// buildScheduleRequest constructs a ScheduleJobRequest for a fresh job submission.
// It uses buildInputSplits to create one Map task per input split and R Reduce tasks.
func buildScheduleRequest(ctx context.Context, storage scheduleObjectClient, jobID, userID string, req models.JobSubmissionRequest, combinerURI string) (manager.ScheduleJobRequest, error) {
	inputURI := fmt.Sprintf("s3://%s/%s", inputBucketName, req.Filename)
	inputSplits := buildInputSplits(ctx, storage, req.Filename, inputURI)

	tasks := make([]manager.ScheduleTask, 0, len(inputSplits)+req.Reducers)
	for _, split := range inputSplits {
		split := split
		tasks = append(tasks, manager.ScheduleTask{
			TaskID:      uuid.New().String(),
			TaskType:    "Map",
			InputSplits: []manager.ScheduleTaskInput{split},
		})
	}
	for i := 0; i < req.Reducers; i++ {
		tasks = append(tasks, manager.ScheduleTask{
			TaskID:       uuid.New().String(),
			TaskType:     "Reduce",
			ReplicaIndex: i,
		})
	}
	return manager.ScheduleJobRequest{
		JobID:         jobID,
		UserID:        userID,
		InputURI:      req.Filename,
		MapperURI:     req.Mapper.Artifact,
		ReducerURI:    req.Reducer.Artifact,
		CombinerURI:   combinerURI,
		MTasks:        1,
		RTasks:        req.Reducers,
		InputChecksum: req.InputChecksum,
		Tasks:         tasks,
	}

	rc, err := storage.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return "", fmt.Errorf("get object range %s/%s [%d,%d]: %w", bucketName, objectName, start, end, err)
	}
	defer rc.Close()

	h := sha256.New()
	n, err := io.Copy(h, rc)
	if err != nil {
		return "", fmt.Errorf("read object range %s/%s [%d,%d]: %w", bucketName, objectName, start, end, err)
	}

	expected := end - start + 1
	if n != expected {
		return "", fmt.Errorf("short read %s/%s [%d,%d]: got %d bytes, expected %d", bucketName, objectName, start, end, n, expected)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func buildInputSplits(ctx context.Context, storage scheduleObjectClient, objectName, inputURI string) []manager.ScheduleTaskInput {
	if storage == nil {
		return []manager.ScheduleTaskInput{{
			InputURI: inputURI,
		}}
	}

	info, err := storage.StatObject(ctx, inputBucketName, objectName, minio.StatObjectOptions{})
	if err != nil || info.Size <= 0 {
		return []manager.ScheduleTaskInput{{
			InputURI: inputURI,
		}}
	}

	splitSize := int64(defaultTargetSplitSizeBytes)
	if splitSize <= 0 {
		splitSize = 64 * 1024 * 1024
	}

	splits := make([]manager.ScheduleTaskInput, 0, int((info.Size+splitSize-1)/splitSize))
	for start := int64(0); start < info.Size; start += splitSize {
		end := start + splitSize - 1
		if end >= info.Size {
			end = info.Size - 1
		}
		checksum, checksumErr := checksumObjectRange(ctx, storage, inputBucketName, objectName, start, end)
		if checksumErr != nil {
			log.Printf("[api] falling back to a single split for %s after checksum error: %v", inputURI, checksumErr)
			return []manager.ScheduleTaskInput{{
				InputURI: inputURI,
			}}
		}
		splits = append(splits, manager.ScheduleTaskInput{
			InputURI:      inputURI,
			ByteStart:     start,
			ByteEnd:       end,
			SplitChecksum: checksum,
		})
	}

	if len(splits) == 0 {
		return []manager.ScheduleTaskInput{{InputURI: inputURI}}
	}
	return splits
}

func currentRequestUserID(r *http.Request) (string, error) {
	return auth.GetSubject(r)
}

// jobMessage derives a human-readable message from the DDS job status.
func jobMessage(status string) string {
	switch status {
	case "Pending":
		return "job specification validated and accepted"
	case "Running":
		return "job is currently running"
	case "Completed":
		return "job completed successfully"
	case "Failed":
		return "job failed"
	case "Cancelled":
		return "job was cancelled"
	case "Cleaning":
		return "job is cleaning up temporary resources"
	default:
		return ""
	}
}

// ── Admin user management ──────────────────────────────────

// HandleAdminCreateUser provisions a new user in the Keycloak identity provider.
//
// This handler acts as a proxy to the [auth.KeycloakAdminClient]. It ensures
// that all users created via the API adhere to the system's role structure
// (e.g., USER vs ADMIN). Returns 201 Created on success.
func (h *Handlers) HandleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.adminClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "authentication admin client not configured")
		return
	}

	var req models.CreateUserRequest
	if !decodeJSONBody(w, r, &req, "invalid request payload") {
		return
	}

	if err := validation.ValidateCreateUserRequest(req); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	normalizedRole := validation.NormalizeRole(req.Role)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.adminClient.CreateUser(ctx, auth.CreateUserRequest{
		Username: req.Username,
		Email:    req.Email,
		Password: req.Password,
		Role:     normalizedRole,
	}); err != nil {
		if isAuthDependencyError(err) {
			httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "authentication service unavailable")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := httputil.WriteJSON(w, http.StatusCreated, map[string]string{
		"status":   "created",
		"username": req.Username,
		"role":     normalizedRole,
	}); err != nil {
		return
	}
}

// HandleAdminDeleteUser removes a user from Keycloak.
//
// It performs a "hard delete" of the user identity. This is an idempotent
// operation: deleting a non-existent user will return success (via the
// underlying client behavior) or a specific error if the auth service fails.
func (h *Handlers) HandleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.adminClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "authentication admin client not configured")
		return
	}

	username := strings.TrimSpace(r.PathValue("username"))
	if username == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "username is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.adminClient.DeleteUserByUsername(ctx, username); err != nil {
		if isAuthDependencyError(err) {
			httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "authentication service unavailable")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// isAuthDependencyError reports whether the error indicates the authentication
// service could not be reached or timed out.
func isAuthDependencyError(err error) bool {
	return auth.IsServiceUnavailable(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled)
}

// HandleJobsDelete initiates the cancellation of a running or pending job.
//
// This operation is proxied to the Manager service's internal endpoint to ensure
// that all active worker processes for the job are terminated and resources
// are released.
func (h *Handlers) HandleJobsDelete(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "job id required")
		return
	}

	if _, err := uuid.Parse(jobID); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var rec *JobRecord
	if auth.HasRole(r, "ADMIN") {
		rec, err = h.store.GetAnyJob(r.Context(), jobID)
	} else {
		rec, err = h.store.GetJob(r.Context(), userID, jobID)
	}
	if err != nil {
		if errors.Is(err, ErrInvalidUserID) {
			httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: invalid subject")
			return
		}
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to retrieve job")
		return
	}
	if rec == nil {
		httputil.WriteErrorJSON(w, http.StatusNotFound, "job not found")
		return
	}

	managerURL := fmt.Sprintf("http://%s/internal/jobs/%s", h.managerAddr, jobID)
	proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodDelete, managerURL, nil)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to build cancellation request")
		return
	}

	if h.internalAPIKey != "" {
		proxyReq.Header.Set("X-Internal-Token", h.internalAPIKey)
	}

	resp, err := h.httpClient.Do(proxyReq)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "manager service unavailable")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		httputil.WriteErrorJSON(w, resp.StatusCode, "cancellation rejected by manager")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// HandlePresignUpload generates a temporary URL for direct file upload to object storage.
func (h *Handlers) HandlePresignUpload(w http.ResponseWriter, r *http.Request) {
	if h.minioClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "object storage not configured")
		return
	}

	var req models.PresignRequest
	if !decodeJSONBody(w, r, &req, "invalid presign request") {
		return
	}

	if req.Bucket == "" || req.Key == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "bucket and key are required")
		return
	}

	url, err := h.minioClient.PresignedPutObject(r.Context(), req.Bucket, req.Key, 15*time.Minute)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to generate upload URL")
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, models.PresignResponse{URL: url.String()}); err != nil {
		return
	}
}

// HandlePresignDownload generates a temporary URL for direct file download from object storage.
func (h *Handlers) HandlePresignDownload(w http.ResponseWriter, r *http.Request) {
	if h.minioClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "object storage not configured")
		return
	}

	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	if bucket == "" || key == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "bucket and key are required")
		return
	}

	url, err := h.minioClient.PresignedGetObject(r.Context(), bucket, key, 15*time.Minute, nil)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to generate download URL")
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, models.PresignResponse{URL: url.String()}); err != nil {
		return
	}
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any, invalidMessage string) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httputil.WriteErrorJSON(w, http.StatusRequestEntityTooLarge, "request payload too large")
			return false
		}
		httputil.WriteErrorJSON(w, http.StatusBadRequest, invalidMessage)
		return false
	}
	if dec.More() {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, invalidMessage)
		return false
	}
	return true
}

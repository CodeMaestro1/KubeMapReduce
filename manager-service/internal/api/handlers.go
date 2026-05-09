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
	"log/slog"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
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
	copier         objectCopier
	managerAddr    string
	internalAPIKey string
	httpClient     *http.Client
	now            func() time.Time
}

const (
	defaultReducers   = 1
	defaultListLimit  = 100
	maxListLimit      = 500
	maxJSONBodyBytes  = 1 << 20 // 1 MiB
	inputBucketName   = "mapreduce-inputs"
	stagingBucketName = "mapreduce-staging"
)

var defaultTargetSplitSizeBytes int64 = 64 * 1024 * 1024

// safeFilenamePattern validates that a filename is a simple basename with no traversal or special chars.
// Only alphanumeric, hyphens, underscores, and single dots in the middle are allowed.
// This prevents patterns like "...", "a..b", or any traversal attempts.
var safeFilenamePattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9_\-]|(?:\.[a-zA-Z0-9_\-]))*[a-zA-Z0-9]?$|^[a-zA-Z0-9]$`)

// NewHandlers creates production-ready Handlers.
func NewHandlers(adminClient *auth.KeycloakAdminClient, store JobStore, minioClient *minio.Client, managerAddr string, internalAPIKey string) *Handlers {
	return &Handlers{
		adminClient:    adminClient,
		store:          store,
		minioClient:    minioClient,
		copier:         newObjectCopier(minioClient),
		managerAddr:    managerAddr,
		internalAPIKey: internalAPIKey,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
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
		copier:         newObjectCopier(minioClient),
		managerAddr:    managerAddr,
		internalAPIKey: internalAPIKey,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
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

// HandleHealthz provides an unauthenticated liveness probe endpoint.
//
// It returns a simple 200 OK status to indicate the web server process is
// running and able to serve requests. It does NOT exercise downstream
// dependencies (database, MinIO, Keycloak); use [Handlers.HandleReadyz] for
// dependency-aware readiness checks.
func (h *Handlers) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"}); err != nil {
		return
	}
}

// HandleReadyz reports whether the API service is ready to serve traffic.
//
// It pings the underlying [JobStore] (PostgreSQL DDS in production) to
// confirm the database connection is healthy. Returns 503 Service Unavailable
// if the store is unreachable, 200 OK otherwise. The DB ping is bounded by a
// short context timeout to prevent the readiness probe from blocking.
func (h *Handlers) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := h.store.Ping(ctx); err != nil {
			httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "database not ready")
			return
		}
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"}); err != nil {
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

	combinerArtifact := ""
	if request.Combiner != nil {
		combinerArtifact = request.Combiner.Artifact
	}

	if h.copier == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "object storage not available")
		return
	}

	promotedInputURI, promotedMapperURI, promotedReducerURI, promotedCombinerURI, err := promoteJobFiles(r.Context(), h.copier, userID, jobID, request, combinerArtifact)
	if err != nil {
		slog.ErrorContext(r.Context(), "failed to promote job files",
			slog.String("job_id", jobID),
			slog.Any("err", err),
		)
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to stage job files")
		return
	}

	schedReq := buildScheduleRequest(r.Context(), newScheduleObjectClient(h.minioClient), jobID, userID, request, promotedInputURI, promotedMapperURI, promotedReducerURI, promotedCombinerURI)

	rec := JobRecord{
		JobID:         jobID,
		UserID:        userID,
		Status:        "Pending",
		Filename:      promotedInputURI,
		InputChecksum: request.InputChecksum,
		Reducers:      request.Reducers,
		CreatedAt:     now,
		MapperURI:     promotedMapperURI,
		ReducerURI:    promotedReducerURI,
		CombinerURI:   promotedCombinerURI,
		MTasks:        schedReq.MTasks,
	}
	if err := h.store.CreateJob(r.Context(), rec); err != nil {
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to persist job")
		return
	}

	if h.managerAddr != "" {
		if err := h.postSchedule(r.Context(), schedReq); err != nil {
			slog.ErrorContext(r.Context(), "job persisted but schedule call failed",
				slog.String("job_id", jobID),
				slog.Any("err", err),
			)
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

// handleJobBatchPresign generates presigned download URLs for all output files
// of a completed job and writes {jobId, urls:[...]} to w.
func (h *Handlers) handleJobBatchPresign(w http.ResponseWriter, r *http.Request, jobID, userID string) {
	if _, err := uuid.Parse(jobID); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "invalid job id")
		return
	}

	var (
		rec *JobRecord
		err error
	)
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
		slog.ErrorContext(r.Context(), "GetJobOutputs failed",
			slog.String("job_id", jobID),
			slog.Any("err", err),
		)
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
			slog.ErrorContext(r.Context(), "invalid output URI",
				slog.String("job_id", jobID),
				slog.String("uri", uri),
				slog.Any("err", parseErr),
			)
			httputil.WriteErrorJSON(w, http.StatusInternalServerError, "invalid output URI")
			return
		}
		u, presignErr := h.minioClient.PresignedGetObject(r.Context(), bucket, key, 15*time.Minute, nil)
		if presignErr != nil {
			slog.ErrorContext(r.Context(), "presign failed for output URI",
				slog.String("job_id", jobID),
				slog.String("uri", uri),
				slog.Any("err", presignErr),
			)
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
		strings.TrimSpace(req.CPULimit) == "" && strings.TrimSpace(req.MemoryLimit) == "" &&
		r.Method != http.MethodPost { // Method check is redundant here but keeping logic flow
		// Actually, we want to allow updating ONLY localityKey if provided.
	}
	// Better check: at least one field must be non-zero or non-empty
	if req.MaxPods <= 0 && req.WorkerReplicas <= 0 && req.MaxJobsPerNode <= 0 &&
		req.CPULimit == "" && req.MemoryLimit == "" && req.LocalityKey == "" &&
		req.LocalityLabelSelector == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "at least one configuration field must be provided")
		return
	}

	update := manager.SystemConfigUpdate{
		MaxConcurrentPods:     req.MaxPods,
		CPULimit:              req.CPULimit,
		MemoryLimit:           req.MemoryLimit,
		WorkerReplicas:        req.WorkerReplicas,
		MaxJobsPerNode:        req.MaxJobsPerNode,
		LocalityKey:           req.LocalityKey,
		LocalityLabelSelector: req.LocalityLabelSelector,
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
		"status":                "accepted",
		"maxPods":               req.MaxPods,
		"cpuLimit":              req.CPULimit,
		"memoryLimit":           req.MemoryLimit,
		"workerReplicas":        req.WorkerReplicas,
		"maxJobsPerNode":        req.MaxJobsPerNode,
		"localityKey":           req.LocalityKey,
		"localityLabelSelector": req.LocalityLabelSelector,
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
	ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]scheduleObjectInfo, error)
}

type scheduleObjectInfo struct {
	Key  string
	Size int64
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

func (m *minioScheduleObjectClient) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]scheduleObjectInfo, error) {
	entries := make([]scheduleObjectInfo, 0)
	for obj := range m.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.TrimSpace(obj.Key) == "" || strings.HasSuffix(obj.Key, "/") {
			continue
		}
		entries = append(entries, scheduleObjectInfo{Key: obj.Key, Size: obj.Size})
	}
	return entries, nil
}

// objectCopier abstracts minio server-side CopyObject for testability.
type objectCopier interface {
	CopyObject(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error)
}

type minioCopier struct{ client *minio.Client }

func (m *minioCopier) CopyObject(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error) {
	return m.client.CopyObject(ctx, dst, src)
}

func newObjectCopier(client *minio.Client) objectCopier {
	if client == nil {
		return nil
	}
	return &minioCopier{client: client}
}

// promoteArtifact copies an artifact from the temp staging area to a permanent job path.
// If artifactURI is an external URI (contains "://"), it is returned unchanged — no copy.
// Otherwise it is treated as a bare filename uploaded to temp/<userID>/<name>.
func promoteArtifact(ctx context.Context, copier objectCopier, userID, jobID, artifactURI string) (string, error) {
	var filename string
	tempPrefix := fmt.Sprintf("s3://%s/temp/%s/", inputBucketName, userID)
	switch {
	case strings.HasPrefix(artifactURI, tempPrefix):
		filename = artifactURI[len(tempPrefix):]
	case !strings.Contains(artifactURI, "://"):
		filename = artifactURI
	default:
		return artifactURI, nil
	}
	srcKey := fmt.Sprintf("temp/%s/%s", userID, filename)
	dstKey := fmt.Sprintf("inputs/%s/%s", jobID, filename)
	src := minio.CopySrcOptions{Bucket: inputBucketName, Object: srcKey}
	dst := minio.CopyDestOptions{Bucket: inputBucketName, Object: dstKey}
	if _, err := copier.CopyObject(ctx, dst, src); err != nil {
		return "", fmt.Errorf("copy %s → %s: %w", srcKey, dstKey, err)
	}
	return fmt.Sprintf("s3://%s/%s", inputBucketName, dstKey), nil
}

// promoteJobFiles copies all uploaded files for a job from temp/ to inputs/<jobID>/.
// Returns the permanent s3:// URIs for input, mapper, reducer, and combiner.
func promoteJobFiles(ctx context.Context, copier objectCopier, userID, jobID string, req models.JobSubmissionRequest, combinerArtifact string) (inputURI, mapperURI, reducerURI, combinerURI string, err error) {
	if inputURI, err = promoteArtifact(ctx, copier, userID, jobID, req.Filename); err != nil {
		return
	}
	if mapperURI, err = promoteArtifact(ctx, copier, userID, jobID, req.Mapper.Artifact); err != nil {
		return
	}
	if reducerURI, err = promoteArtifact(ctx, copier, userID, jobID, req.Reducer.Artifact); err != nil {
		return
	}
	if combinerArtifact != "" {
		combinerURI, err = promoteArtifact(ctx, copier, userID, jobID, combinerArtifact)
	}
	return
}

// buildScheduleRequest constructs a ScheduleJobRequest for a fresh job submission.
// It uses buildInputBuckets to create one Map task per input bucket and R Reduce tasks.
// Checksum/stat errors are handled best-effort inside buildInputBuckets (fallback to single split).
// inputURI, mapperURI, reducerURI, combinerURI must be pre-resolved permanent s3:// URIs.
func buildScheduleRequest(ctx context.Context, storage scheduleObjectClient, jobID, userID string, req models.JobSubmissionRequest, inputURI, mapperURI, reducerURI, combinerURI string) manager.ScheduleJobRequest {
	objectName := strings.TrimPrefix(inputURI, fmt.Sprintf("s3://%s/", inputBucketName))
	inputBuckets := buildInputBuckets(ctx, storage, objectName, inputURI)

	tasks := make([]manager.ScheduleTask, 0, len(inputBuckets)+req.Reducers)
	for _, bucket := range inputBuckets {
		bucket := bucket
		tasks = append(tasks, manager.ScheduleTask{
			TaskID:      uuid.New().String(),
			TaskType:    "Map",
			InputSplits: bucket,
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
		InputURI:      inputURI,
		MapperURI:     mapperURI,
		ReducerURI:    reducerURI,
		CombinerURI:   combinerURI,
		MTasks:        len(inputBuckets),
		RTasks:        req.Reducers,
		InputChecksum: req.InputChecksum,
		Tasks:         tasks,
	}
}

func buildInputBuckets(ctx context.Context, storage scheduleObjectClient, objectName, inputURI string) [][]manager.ScheduleTaskInput {
	if strings.HasSuffix(objectName, "/") {
		return buildPrefixInputBuckets(ctx, storage, objectName, inputURI)
	}

	single := buildInputSplits(ctx, storage, objectName, inputURI)
	buckets := make([][]manager.ScheduleTaskInput, 0, len(single))
	for _, split := range single {
		split := split
		buckets = append(buckets, []manager.ScheduleTaskInput{split})
	}
	if len(buckets) == 0 {
		return [][]manager.ScheduleTaskInput{{{InputURI: inputURI}}}
	}
	return buckets
}

func buildPrefixInputBuckets(ctx context.Context, storage scheduleObjectClient, prefix, fallbackInputURI string) [][]manager.ScheduleTaskInput {
	if storage == nil {
		return [][]manager.ScheduleTaskInput{{{InputURI: fallbackInputURI}}}
	}

	objects, err := storage.ListObjects(ctx, inputBucketName, prefix, true)
	if err != nil || len(objects) == 0 {
		return [][]manager.ScheduleTaskInput{{{InputURI: fallbackInputURI}}}
	}

	totalSize := int64(0)
	for _, obj := range objects {
		if obj.Size > 0 {
			totalSize += obj.Size
		}
	}

	splitSize := int64(defaultTargetSplitSizeBytes)
	if splitSize <= 0 {
		splitSize = 64 * 1024 * 1024
	}

	mTasks := int((totalSize + splitSize - 1) / splitSize)
	if mTasks < 1 {
		mTasks = 1
	}
	if mTasks > len(objects) {
		mTasks = len(objects)
	}

	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Size == objects[j].Size {
			return objects[i].Key < objects[j].Key
		}
		return objects[i].Size > objects[j].Size
	})

	buckets := make([][]manager.ScheduleTaskInput, mTasks)
	bucketSizes := make([]int64, mTasks)

	for _, obj := range objects {
		bucketIndex := 0
		for i := 1; i < len(bucketSizes); i++ {
			if bucketSizes[i] < bucketSizes[bucketIndex] {
				bucketIndex = i
			}
		}

		split := manager.ScheduleTaskInput{
			InputURI: fmt.Sprintf("s3://%s/%s", inputBucketName, obj.Key),
		}
		if obj.Size > 0 {
			split.ByteStart = 0
			split.ByteEnd = obj.Size - 1
			checksum, checksumErr := checksumObjectRange(ctx, storage, inputBucketName, obj.Key, 0, obj.Size-1)
			if checksumErr != nil {
				slog.WarnContext(ctx, "falling back to a single split after checksum error",
					slog.String("input_uri", fallbackInputURI),
					slog.String("object", obj.Key),
					slog.Any("err", checksumErr),
				)
				return [][]manager.ScheduleTaskInput{{{InputURI: fallbackInputURI}}}
			}
			split.SplitChecksum = checksum
		}

		buckets[bucketIndex] = append(buckets[bucketIndex], split)
		if obj.Size > 0 {
			bucketSizes[bucketIndex] += obj.Size
		}
	}

	result := make([][]manager.ScheduleTaskInput, 0, len(buckets))
	for _, bucket := range buckets {
		if len(bucket) > 0 {
			result = append(result, bucket)
		}
	}
	if len(result) == 0 {
		return [][]manager.ScheduleTaskInput{{{InputURI: fallbackInputURI}}}
	}

	return result
}

func checksumObjectRange(ctx context.Context, storage scheduleObjectClient, bucketName, objectName string, start, end int64) (string, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(start, end); err != nil {
		return "", fmt.Errorf("set range [%d,%d]: %w", start, end, err)
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
			slog.WarnContext(ctx, "falling back to a single split after checksum error",
				slog.String("input_uri", inputURI),
				slog.Any("err", checksumErr),
			)
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

// presignURLTTL is the validity window for issued pre-signed URLs.
const presignURLTTL = 15 * time.Minute

// outputsBucketName is the server-controlled bucket holding finalized job outputs.
// Pre-signed download URLs may only target objects in this bucket.
const outputsBucketName = "mapreduce-outputs"

// validateUploadKey enforces that an upload key is exactly "temp/<userID>/<filename>".
// userID must equal the authenticated subject and filename must contain no path
// separators, traversal segments, or control characters. This prevents an
// authenticated caller from minting pre-signed URLs that write outside their
// own scope (issue #116).
func validateUploadKey(key, userID string) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}
	if len(key) > 512 {
		return fmt.Errorf("key too long")
	}
	if strings.ContainsAny(key, "\x00\r\n") {
		return fmt.Errorf("key contains invalid characters")
	}
	expectedPrefix := "temp/" + userID + "/"
	if !strings.HasPrefix(key, expectedPrefix) {
		return fmt.Errorf("key must start with %q", expectedPrefix)
	}
	filename := key[len(expectedPrefix):]
	if filename == "" {
		return fmt.Errorf("key must include a filename")
	}
	if strings.ContainsAny(filename, "/\\") {
		return fmt.Errorf("key filename must not contain path separators")
	}
	// Validate filename matches safe pattern (alphanumeric, hyphen, underscore, dots).
	// Rejects patterns like ".", "..", "...", "a..b", etc.
	if !safeFilenamePattern.MatchString(filename) {
		return fmt.Errorf("key filename contains invalid characters or pattern")
	}

	cleanPath := filepath.Clean(key)
	if !strings.HasPrefix(cleanPath, expectedPrefix) || filepath.ToSlash(cleanPath) != key {
		return fmt.Errorf("key contains path traversal")
	}

	return nil
}

// validateDownloadKey enforces that a download key starts with "outputs/<jobID>/"
// and contains no traversal segments. Returns the parsed jobID on success.
func validateDownloadKey(key string) (jobID string, err error) {
	if key == "" {
		return "", fmt.Errorf("key is required")
	}
	if len(key) > 512 {
		return "", fmt.Errorf("key too long")
	}
	if strings.ContainsAny(key, "\x00\r\n") {
		return "", fmt.Errorf("key contains invalid characters")
	}
	if strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("key must not start with '/'")
	}
	parts := strings.Split(key, "/")
	if len(parts) < 3 || parts[0] != "outputs" {
		return "", fmt.Errorf("key must start with 'outputs/<job_id>/'")
	}
	jobID = parts[1]
	if _, parseErr := uuid.Parse(jobID); parseErr != nil {
		return "", fmt.Errorf("invalid job id in key")
	}
	for _, seg := range parts {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("key contains invalid path segment")
		}
	}

	cleanPath := filepath.Clean(key)
	if filepath.ToSlash(cleanPath) != key {
		return "", fmt.Errorf("key contains path traversal")
	}

	return jobID, nil
}

// HandlePresignUpload generates a temporary URL for direct file upload to
// object storage.
//
// Security (issue #116):
//   - The destination bucket is server-controlled (mapreduce-inputs); any
//     bucket value supplied by the client is ignored.
//   - The object key must match "temp/<authenticated-user-id>/<filename>".
//     Cross-tenant or staging/output paths are rejected.
//   - Each issuance is audit-logged with subject, bucket, key prefix, and TTL.
func (h *Handlers) HandlePresignUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.minioClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "object storage not configured")
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil || userID == "" {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var req models.PresignRequest
	if !decodeJSONBody(w, r, &req, "invalid presign request") {
		return
	}

	if err := validateUploadKey(req.Key, userID); err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	bucket := inputBucketName
	url, err := h.minioClient.PresignedPutObject(r.Context(), bucket, req.Key, presignURLTTL)
	if err != nil {
		slog.ErrorContext(r.Context(), "presign upload failed",
			slog.String("user_id", userID),
			slog.String("bucket", bucket),
			slog.Any("err", err),
		)
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to generate upload URL")
		return
	}

	slog.InfoContext(r.Context(), "presign upload issued",
		slog.String("user_id", userID),
		slog.String("bucket", bucket),
		slog.String("key_prefix", "temp/"+userID+"/"),
		slog.Duration("ttl", presignURLTTL),
	)

	if err := httputil.WriteJSON(w, http.StatusOK, models.PresignResponse{URL: url.String()}); err != nil {
		return
	}
}

// HandlePresignDownload generates a temporary URL for direct file download from
// object storage.
//
// Security (issue #116):
//   - The source bucket is server-controlled (mapreduce-outputs); any bucket
//     value supplied by the client is ignored.
//   - The object key must match "outputs/<job_id>/...". The caller must own
//     the referenced job (admins are exempt).
//   - Each issuance is audit-logged with subject, bucket, job id, and TTL.
func (h *Handlers) HandlePresignDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	userID, err := currentRequestUserID(r)
	if err != nil || userID == "" {
		httputil.WriteErrorJSON(w, http.StatusForbidden, "forbidden: authenticated subject required")
		return
	}

	var req models.PresignRequest
	if !decodeJSONBody(w, r, &req, "invalid presign request") {
		return
	}

	if req.JobID != "" {
		h.handleJobBatchPresign(w, r, req.JobID, userID)
		return
	}

	if req.Key == "" {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, "key or job_id required")
		return
	}

	if h.minioClient == nil {
		httputil.WriteErrorJSON(w, http.StatusServiceUnavailable, "object storage not configured")
		return
	}

	key := req.Key
	jobID, err := validateDownloadKey(key)
	if err != nil {
		httputil.WriteErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// Ownership check: non-admins may only presign objects under jobs they own.
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
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to verify job ownership")
		return
	}
	if rec == nil {
		httputil.WriteErrorJSON(w, http.StatusNotFound, "job not found")
		return
	}

	bucket := outputsBucketName
	url, err := h.minioClient.PresignedGetObject(r.Context(), bucket, key, presignURLTTL, nil)
	if err != nil {
		slog.ErrorContext(r.Context(), "presign download failed",
			slog.String("user_id", userID),
			slog.String("job_id", jobID),
			slog.String("bucket", bucket),
			slog.Any("err", err),
		)
		httputil.WriteErrorJSON(w, http.StatusInternalServerError, "failed to generate download URL")
		return
	}

	slog.InfoContext(r.Context(), "presign download issued",
		slog.String("user_id", userID),
		slog.String("job_id", jobID),
		slog.String("bucket", bucket),
		slog.Duration("ttl", presignURLTTL),
	)

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

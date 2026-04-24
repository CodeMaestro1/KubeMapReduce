package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

// Handlers holds HTTP handler state for the API server.
//
// This centralizes state management for all REST endpoints, including the
// Keycloak admin client for user management and a [JobStore] for persisting
// job metadata. Using a struct-based handler pattern allows for easy
// dependency injection, making the API layer highly testable with mocks.
type Handlers struct {
	adminClient    *auth.KeycloakAdminClient
	store          JobStore
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
)

// NewHandlers creates production-ready Handlers backed by the given JobStore.
//
// Callers must provide a valid [JobStore] implementation. In production, this
// is typically a [PostgresJobStore] to ensure that job state persists across
// manager restarts and is visible to all replicas.
func NewHandlers(adminClient *auth.KeycloakAdminClient, store JobStore, managerAddr string, internalAPIKey string) *Handlers {
	return &Handlers{
		adminClient:    adminClient,
		store:          store,
		managerAddr:    managerAddr,
		internalAPIKey: internalAPIKey,
		httpClient:     http.DefaultClient,
		now:            time.Now,
	}
}

func newHandlersWithOptions(adminClient *auth.KeycloakAdminClient, store JobStore, managerAddr string, internalAPIKey string, now func() time.Time) *Handlers {
	if now == nil {
		now = time.Now
	}
	return &Handlers{
		adminClient:    adminClient,
		store:          store,
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobID := uuid.New().String()
	now := h.now().UTC()

	rec := JobRecord{
		JobID:     jobID,
		Status:    "Pending",
		Filename:  request.Filename,
		Reducers:  request.Reducers,
		CreatedAt: now,
	}
	if err := h.store.CreateJob(r.Context(), rec); err != nil {
		http.Error(w, "failed to persist job", http.StatusInternalServerError)
		return
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit, offset, err := parsePagination(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	records, err := h.store.ListJobs(r.Context(), limit, offset)
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := r.PathValue("job_id")
	if jobID == "" {
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(jobID); err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	rec, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, ErrInvalidJobID) {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}
		http.Error(w, "failed to retrieve job", http.StatusInternalServerError)
		return
	}
	if rec == nil {
		http.Error(w, "job not found", http.StatusNotFound)
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
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(jobID); err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	rec, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, ErrInvalidJobID) {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}
		http.Error(w, "failed to retrieve job", http.StatusInternalServerError)
		return
	}
	if rec == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusNotImplemented, map[string]interface{}{
		"status":  "not_implemented",
		"message": "result download is not available yet; job processing backend is not implemented",
		"jobId":   rec.JobID,
	}); err != nil {
		return
	}
}

// HandleConfigureNodes updates resource limits for the MapReduce cluster nodes.
//
// This is an administrative endpoint used to fine-tune the maximum pod density
// and resource quotas (CPU/Memory) across the compute fleet. It currently
// returns 501 Not Implemented as the integration with the cluster scheduler
// is still in progress.
func (h *Handlers) HandleConfigureNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.NodeConfigRequest
	if !decodeJSONBody(w, r, &req, "invalid node config payload") {
		return
	}

	if req.MaxPods < 1 {
		http.Error(w, "maxPods must be a positive integer", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.CPULimit) == "" {
		http.Error(w, "cpuLimit is required", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.MemoryLimit) == "" {
		http.Error(w, "memoryLimit is required", http.StatusBadRequest)
		return
	}

	// proxy to manager internal endpoint
	update := manager.SystemConfigUpdate{
		MaxConcurrentPods: req.MaxPods,
		CPULimit:          req.CPULimit,
		MemoryLimit:       req.MemoryLimit,
	}

	payload, _ := json.Marshal(update)
	managerURL := fmt.Sprintf("http://%s/internal/config", h.managerAddr)
	proxyReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPut, managerURL, bytes.NewReader(payload))
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request models.WorkerConfigRequest
	if !decodeJSONBody(w, r, &request, "invalid worker config payload") {
		return
	}

	if request.WorkerReplicas < 1 {
		http.Error(w, "workerReplicas must be positive", http.StatusBadRequest)
		return
	}
	if request.MaxJobsPerNode < 1 {
		http.Error(w, "maxJobsPerNode must be positive", http.StatusBadRequest)
		return
	}

	// proxy to manager internal endpoint
	update := manager.SystemConfigUpdate{
		WorkerReplicas: request.WorkerReplicas,
		MaxJobsPerNode: request.MaxJobsPerNode,
	}

	payload, _ := json.Marshal(update)
	managerURL := fmt.Sprintf("http://%s/internal/config", h.managerAddr)
	proxyReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPut, managerURL, bytes.NewReader(payload))
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.adminClient == nil {
		http.Error(w, "authentication admin client not configured", http.StatusServiceUnavailable)
		return
	}

	var req models.CreateUserRequest
	if !decodeJSONBody(w, r, &req, "invalid request payload") {
		return
	}

	if err := validation.ValidateCreateUserRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
			http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.adminClient == nil {
		http.Error(w, "authentication admin client not configured", http.StatusServiceUnavailable)
		return
	}

	username := strings.TrimSpace(r.PathValue("username"))
	if username == "" {
		http.Error(w, "username is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.adminClient.DeleteUserByUsername(ctx, username); err != nil {
		if isAuthDependencyError(err) {
			http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any, invalidMessage string) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(out); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request payload too large", http.StatusRequestEntityTooLarge)
			return false
		}
		http.Error(w, invalidMessage, http.StatusBadRequest)
		return false
	}
	return true
}

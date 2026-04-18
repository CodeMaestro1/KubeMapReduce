package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/internal/models"
	"kubemapreduce/manager-service/internal/validation"
	"kubemapreduce/manager-service/pkg/httputil"

	"github.com/google/uuid"
)

// Handlers holds HTTP handler state for the API server.
// Job storage is delegated to a JobStore implementation, which is backed
// by PostgreSQL in production for replica-safe, persistent state.
type Handlers struct {
	adminClient *auth.KeycloakAdminClient
	store       JobStore
	now         func() time.Time
}

const (
	defaultReducers  = 1
	defaultListLimit = 100
	maxListLimit     = 500
)

// NewHandlers creates production-ready Handlers backed by the given JobStore.
func NewHandlers(adminClient *auth.KeycloakAdminClient, store JobStore) *Handlers {
	return &Handlers{
		adminClient: adminClient,
		store:       store,
		now:         time.Now,
	}
}

func newHandlersWithOptions(adminClient *auth.KeycloakAdminClient, store JobStore, now func() time.Time) *Handlers {
	if now == nil {
		now = time.Now
	}
	return &Handlers{
		adminClient: adminClient,
		store:       store,
		now:         now,
	}
}

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

func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"}); err != nil {
		return
	}
}

func (h *Handlers) HandleJobsSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request models.JobSubmissionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid job payload", http.StatusBadRequest)
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

	rec, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
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

func (h *Handlers) HandleJobsDownload(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}

	rec, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
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

func (h *Handlers) HandleConfigureNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.NodeConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid node config payload", http.StatusBadRequest)
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

	if err := httputil.WriteJSON(w, http.StatusNotImplemented, map[string]interface{}{
		"status":      "not_implemented",
		"message":     "node configuration backend integration is not implemented yet",
		"maxPods":     req.MaxPods,
		"cpuLimit":    req.CPULimit,
		"memoryLimit": req.MemoryLimit,
	}); err != nil {
		return
	}
}

func (h *Handlers) HandleWorkerConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request models.WorkerConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid worker config payload", http.StatusBadRequest)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
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

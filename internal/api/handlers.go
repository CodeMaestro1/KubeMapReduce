package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"kubemapreduce/internal/models"
	"kubemapreduce/internal/validation"
	"kubemapreduce/pkg/auth"
	"kubemapreduce/pkg/httputil"
)

// Handlers holds HTTP handler state for the API server.
//
// TEMPORARY: Job storage uses an in-memory sync.Map. This means:
//   - All job data is lost on server restart.
//   - Job visibility is not shared across multiple replicas.
//
// This will be replaced with a persistent store (e.g. database) in a future release.
type Handlers struct {
	adminClient   *auth.KeycloakAdminClient
	jobs          sync.Map // key: string (jobID) → models.JobStatusResponse [interim: in-memory only]
	jobsMu        sync.Mutex
	jobStatusTTL  time.Duration
	maxStoredJobs int
	now           func() time.Time
}

const (
	defaultReducers      = 1
	defaultJobStatusTTL  = 24 * time.Hour
	defaultMaxStoredJobs = 10000
)

func NewHandlers(adminClient *auth.KeycloakAdminClient) *Handlers {
	return newHandlersWithOptions(adminClient, defaultJobStatusTTL, defaultMaxStoredJobs, time.Now)
}

func newHandlersWithOptions(adminClient *auth.KeycloakAdminClient, jobStatusTTL time.Duration, maxStoredJobs int, now func() time.Time) *Handlers {
	if jobStatusTTL <= 0 {
		jobStatusTTL = defaultJobStatusTTL
	}
	if maxStoredJobs <= 0 {
		maxStoredJobs = defaultMaxStoredJobs
	}
	if now == nil {
		now = time.Now
	}

	return &Handlers{
		adminClient:   adminClient,
		jobStatusTTL:  jobStatusTTL,
		maxStoredJobs: maxStoredJobs,
		now:           now,
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

	h.cleanupJobsStore()

	if err := validation.ValidateJobSubmission(request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobID, err := generateJobID()
	if err != nil {
		http.Error(w, "failed to create job id", http.StatusInternalServerError)
		return
	}

	now := h.now().UTC()
	jobStatus := models.JobStatusResponse{
		JobID:     jobID,
		Status:    "accepted",
		Message:   "job specification validated and accepted (metadata only — no file transfer)",
		Filename:  request.Filename,
		Reducers:  request.Reducers,
		CreatedAt: now,
	}
	h.jobs.Store(jobID, jobStatus)
	h.cleanupJobsStore()

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

	h.cleanupJobsStore()

	var list []models.JobStatusResponse
	h.jobs.Range(func(_, v any) bool {
		list = append(list, v.(models.JobStatusResponse))
		return true
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
	if list == nil {
		list = []models.JobStatusResponse{}
	}

	if err := httputil.WriteJSON(w, http.StatusOK, list); err != nil {
		return
	}
}

func (h *Handlers) HandleJobsGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.cleanupJobsStore()

	jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if jobID == "" {
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}

	v, ok := h.jobs.Load(jobID)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, v.(models.JobStatusResponse)); err != nil {
		return
	}
}

func (h *Handlers) HandleJobsDownload(w http.ResponseWriter, r *http.Request) {
	h.cleanupJobsStore()

	jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
	jobID = strings.TrimSuffix(jobID, "/results")
	if jobID == "" {
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}

	_, ok := h.jobs.Load(jobID)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusNotImplemented, map[string]interface{}{
		"status":  "not_implemented",
		"message": "result download is not available yet; job processing backend is not implemented",
		"jobId":   jobID,
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

	if request.MaxConcurrentPods < 1 {
		http.Error(w, "maxConcurrentPods must be positive", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.CPULimit) == "" {
		http.Error(w, "cpuLimit must be non-empty", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.MemoryLimit) == "" {
		http.Error(w, "memoryLimit must be non-empty", http.StatusBadRequest)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":            "accepted",
		"message":           "worker configuration update accepted",
		"maxConcurrentPods": request.MaxConcurrentPods,
		"cpuLimit":          request.CPULimit,
		"memoryLimit":       request.MemoryLimit,
	}); err != nil {
		return
	}
}

func generateJobID() (string, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "job-" + hex.EncodeToString(raw), nil
}

func (h *Handlers) cleanupJobsStore() {
	h.jobsMu.Lock()
	defer h.jobsMu.Unlock()

	now := h.now().UTC()
	h.jobs.Range(func(k, v any) bool {
		jobID, ok := k.(string)
		if !ok {
			return true
		}
		status, ok := v.(models.JobStatusResponse)
		if !ok {
			h.jobs.Delete(jobID)
			return true
		}
		if now.Sub(status.CreatedAt) > h.jobStatusTTL {
			h.jobs.Delete(jobID)
		}
		return true
	})

	if h.maxStoredJobs <= 0 {
		return
	}

	type jobRecord struct {
		jobID   string
		created time.Time
	}

	records := make([]jobRecord, 0)
	h.jobs.Range(func(k, v any) bool {
		jobID, ok := k.(string)
		if !ok {
			return true
		}
		status, ok := v.(models.JobStatusResponse)
		if !ok {
			h.jobs.Delete(jobID)
			return true
		}
		records = append(records, jobRecord{jobID: jobID, created: status.CreatedAt})
		return true
	})

	if len(records) <= h.maxStoredJobs {
		return
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].created.Before(records[j].created)
	})

	for _, record := range records[:len(records)-h.maxStoredJobs] {
		h.jobs.Delete(record.jobID)
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

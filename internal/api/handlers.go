package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

type Handlers struct {
	adminClient *auth.KeycloakAdminClient
	jobs        sync.Map // key: string (jobID) → models.JobStatus
}

func NewHandlers(adminClient *auth.KeycloakAdminClient) *Handlers {
	return &Handlers{adminClient: adminClient}
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

	if err := validation.ValidateJobSubmission(request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobID, err := generateJobID()
	if err != nil {
		http.Error(w, "failed to create job id", http.StatusInternalServerError)
		return
	}

	now := time.Now().UTC()
	jobStatus := models.JobStatus{
		JobID:     jobID,
		Status:    "accepted",
		Message:   "job specification validated and accepted",
		Filename:  request.Filename,
		Reducers:  request.Reducers,
		CreatedAt: now,
	}
	h.jobs.Store(jobID, jobStatus)

	response := models.JobSubmissionResponse{
		JobID:   jobID,
		Status:  "accepted",
		Message: "job specification validated and accepted",
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

	var list []models.JobStatus
	h.jobs.Range(func(_, v any) bool {
		list = append(list, v.(models.JobStatus))
		return true
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
	if list == nil {
		list = []models.JobStatus{}
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

	if err := httputil.WriteJSON(w, http.StatusOK, v.(models.JobStatus)); err != nil {
		return
	}
}

func (h *Handlers) HandleJobsDownload(w http.ResponseWriter, r *http.Request) {
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

	if request.WorkerReplicas < 1 || request.MaxJobsPerNode < 1 {
		http.Error(w, "workerReplicas and maxJobsPerNode must be positive", http.StatusBadRequest)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusNotImplemented, map[string]interface{}{
		"status":         "not_implemented",
		"message":        "worker configuration backend integration is not implemented yet",
		"workerReplicas": request.WorkerReplicas,
		"maxJobsPerNode": request.MaxJobsPerNode,
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

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.adminClient.CreateUser(ctx, auth.CreateUserRequest{
		Username: req.Username,
		Email:    req.Email,
		Password: req.Password,
		Role:     req.Role,
	}); err != nil {
		if auth.IsServiceUnavailable(err) {
			http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusCreated, map[string]string{
		"status":   "created",
		"username": req.Username,
		"role":     req.Role,
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

	var req models.DeleteUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	if req.Username == "" {
		http.Error(w, "username is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.adminClient.DeleteUserByUsername(ctx, req.Username); err != nil {
		if auth.IsServiceUnavailable(err) {
			http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"status":   "deleted",
		"username": req.Username,
	}); err != nil {
		return
	}
}

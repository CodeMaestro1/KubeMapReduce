package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"kubemapreduce/internal/models"
	"kubemapreduce/internal/validation"
	"kubemapreduce/pkg/auth"
	"kubemapreduce/pkg/httputil"
)

type Handlers struct {
	adminClient *auth.KeycloakAdminClient
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

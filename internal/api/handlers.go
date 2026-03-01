package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"kubemapreduce/internal/models"
	"kubemapreduce/internal/validation"
	"kubemapreduce/pkg/auth"
	"kubemapreduce/pkg/httputil"
)

type Handlers struct {
	adminClient *auth.KeycloakAdminClient
	uiHTML      string
	keycloakCfg UIConfig
}

type UIConfig struct {
	KeycloakBaseURL string
	Realm           string
	ClientID        string
}

func NewHandlers(adminClient *auth.KeycloakAdminClient, uiHTML string, cfg UIConfig) *Handlers {
	return &Handlers{
		adminClient: adminClient,
		uiHTML:      uiHTML,
		keycloakCfg: cfg,
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(h.uiHTML))
}

func (h *Handlers) HandleUIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"keycloakBaseUrl": h.keycloakCfg.KeycloakBaseURL,
		"realm":           h.keycloakCfg.Realm,
		"clientId":        h.keycloakCfg.ClientID,
	})
}

func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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

	response := models.JobSubmissionResponse{
		JobID:   jobID,
		Status:  "accepted",
		Message: "job specification validated and accepted",
	}

	httputil.WriteJSON(w, http.StatusAccepted, response)
}

func (h *Handlers) HandleCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request models.CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid user payload", http.StatusBadRequest)
		return
	}

	if err := validation.ValidateCreateUserRequest(request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.adminClient.CreateUser(auth.CreateUserRequest{
		Username: request.Username,
		Email:    request.Email,
		Password: request.Password,
		Role:     request.Role,
	}); err != nil {
		http.Error(w, "failed to create user via authentication service: "+err.Error(), http.StatusBadGateway)
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, map[string]string{
		"status":   "created",
		"username": request.Username,
		"role":     request.Role,
	})
}

func (h *Handlers) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	username := strings.TrimPrefix(r.URL.Path, "/admin/users/")
	if username == "" {
		http.Error(w, "username is required in path", http.StatusBadRequest)
		return
	}

	if err := h.adminClient.DeleteUserByUsername(username); err != nil {
		http.Error(w, "failed to delete user via authentication service: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
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

	httputil.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"workerReplicas": request.WorkerReplicas,
		"maxJobsPerNode": request.MaxJobsPerNode,
	})
}

func generateJobID() (string, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "job-" + hex.EncodeToString(raw), nil
}

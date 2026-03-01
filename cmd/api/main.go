package main

import (
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"kubemapreduce/pkg/auth"
)

type FunctionSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

type JobSubmissionRequest struct {
	Filename string       `json:"filename"`
	Mapper   FunctionSpec `json:"mapper"`
	Reducer  FunctionSpec `json:"reducer"`
}

type JobSubmissionResponse struct {
	JobID   string `json:"jobId"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type CreateUserRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

type WorkerConfigRequest struct {
	WorkerReplicas int `json:"workerReplicas"`
	MaxJobsPerNode int `json:"maxJobsPerNode"`
}

const (
	mapInterface    = "map(key,value)->[]KeyValue"
	reduceInterface = "reduce(key,values)->Value"
)

var allowedLanguages = map[string]struct{}{
	"python": {},
	"java":   {},
	"c":      {},
	"cpp":    {},
}

//go:embed ui/index.html
var authUIHTML string

func main() {

	keycloakBaseURL := getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080")
	realm := getEnv("KEYCLOAK_REALM", "mapreduce")
	jwksURL := getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs")
	issuer := getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm)
	audience := getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api")

	validator, err := auth.NewJWTValidator(jwksURL, issuer, audience)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): Keycloak returned 404. Ensure realm %q and client %q exist, or override KEYCLOAK_REALM/KEYCLOAK_JWKS_URL/KEYCLOAK_ISSUER. Original error: %v", jwksURL, issuer, audience, realm, audience, err)
		}

		log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): %v", jwksURL, issuer, audience, err)
	}

	adminClient := auth.NewKeycloakAdminClient(
		keycloakBaseURL,
		realm,
		os.Getenv("KEYCLOAK_ADMIN_USERNAME"),
		os.Getenv("KEYCLOAK_ADMIN_PASSWORD"),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(authUIHTML))
	})

	mux.HandleFunc("/ui/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"keycloakBaseUrl": keycloakBaseURL,
			"realm":           realm,
			"clientId":        audience,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.Handle("/jobs", auth.RequireAnyRole([]string{"USER", "ADMIN"}, validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request JobSubmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid job payload", http.StatusBadRequest)
			return
		}

		if err := validateJobSubmission(request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		jobID, err := generateJobID()
		if err != nil {
			http.Error(w, "failed to create job id", http.StatusInternalServerError)
			return
		}

		response := JobSubmissionResponse{
			JobID:   jobID,
			Status:  "accepted",
			Message: "job specification validated and accepted",
		}

		writeJSON(w, http.StatusAccepted, response)
	})))

	mux.Handle("/admin/users", auth.RequireRole("ADMIN", validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request CreateUserRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid user payload", http.StatusBadRequest)
			return
		}

		request.Role = strings.ToUpper(strings.TrimSpace(request.Role))

		if err := validateCreateUserRequest(request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := adminClient.CreateUser(auth.CreateUserRequest{
			Username: request.Username,
			Email:    request.Email,
			Password: request.Password,
			Role:     request.Role,
		}); err != nil {
			http.Error(w, "failed to create user via authentication service: "+err.Error(), http.StatusBadGateway)
			return
		}

		writeJSON(w, http.StatusCreated, map[string]string{
			"status":   "created",
			"username": request.Username,
			"role":     request.Role,
		})
	})))

	mux.Handle("/admin/users/", auth.RequireRole("ADMIN", validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		username := strings.TrimPrefix(r.URL.Path, "/admin/users/")
		if username == "" {
			http.Error(w, "username is required in path", http.StatusBadRequest)
			return
		}

		if err := adminClient.DeleteUserByUsername(username); err != nil {
			http.Error(w, "failed to delete user via authentication service: "+err.Error(), http.StatusBadGateway)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})))

	mux.Handle("/admin/workers/config", auth.RequireRole("ADMIN", validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request WorkerConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid worker config payload", http.StatusBadRequest)
			return
		}

		if request.WorkerReplicas < 1 || request.MaxJobsPerNode < 1 {
			http.Error(w, "workerReplicas and maxJobsPerNode must be positive", http.StatusBadRequest)
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":         "accepted",
			"workerReplicas": request.WorkerReplicas,
			"maxJobsPerNode": request.MaxJobsPerNode,
		})
	})))

	log.Println("API running on :8081")
	log.Fatal(http.ListenAndServe(":8081", mux))
}

func validateJobSubmission(request JobSubmissionRequest) error {
	if request.Filename == "" {
		return errBadRequest("filename is required")
	}

	clean := filepath.Clean(request.Filename)
	if strings.Contains(clean, "..") || clean == "." {
		return errBadRequest("filename is invalid")
	}

	if err := validateFunctionSpec("mapper", request.Mapper, mapInterface); err != nil {
		return err
	}

	if err := validateFunctionSpec("reducer", request.Reducer, reduceInterface); err != nil {
		return err
	}

	return nil
}

func validateFunctionSpec(functionName string, spec FunctionSpec, expectedInterface string) error {
	language := strings.ToLower(strings.TrimSpace(spec.Language))
	if _, ok := allowedLanguages[language]; !ok {
		return errBadRequest(functionName + ".language must be one of: python, java, c, cpp")
	}

	if strings.TrimSpace(spec.Artifact) == "" {
		return errBadRequest(functionName + ".artifact is required")
	}

	if strings.TrimSpace(spec.Entrypoint) == "" {
		return errBadRequest(functionName + ".entrypoint is required")
	}

	if strings.TrimSpace(spec.Interface) != expectedInterface {
		return errBadRequest(functionName + ".interface must be " + expectedInterface)
	}

	return nil
}

func validateCreateUserRequest(request CreateUserRequest) error {
	if request.Username == "" {
		return errBadRequest("username is required")
	}

	if request.Password == "" {
		return errBadRequest("password is required")
	}

	role := strings.ToUpper(request.Role)
	if role != "USER" && role != "ADMIN" {
		return errBadRequest("role must be USER or ADMIN")
	}

	return nil
}

func generateJobID() (string, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}

	return "job-" + hex.EncodeToString(raw), nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func errBadRequest(message string) error {
	return &badRequestError{message: message}
}

type badRequestError struct {
	message string
}

func (e *badRequestError) Error() string {
	return e.message
}

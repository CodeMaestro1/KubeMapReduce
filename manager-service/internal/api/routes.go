package api

import (
	"net/http"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/pkg/httputil"
)

// RegisterRoutes wires up the HTTP handlers to their respective URL patterns.
func RegisterRoutes(mux *http.ServeMux, h *Handlers, validator *auth.JWTValidator) {
	// Public routes
	mux.HandleFunc("/", h.HandleRoot)
	mux.HandleFunc("/health", h.HandleHealth)

	// Authenticated routes
	mux.Handle("/api/v1/jobs", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				h.HandleJobsSubmit(w, r)
			case http.MethodGet:
				h.HandleJobsList(w, r)
			default:
				httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		}),
	))

	mux.Handle("DELETE /api/v1/jobs/{job_id}", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsDelete),
	))

	mux.Handle("GET /api/v1/jobs/{job_id}/results", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsDownload),
	))

	mux.Handle("GET /api/v1/jobs/{job_id}", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsGet),
	))

	// File Management
	mux.Handle("POST /api/v1/files/presign-upload", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandlePresignUpload),
	))

	mux.Handle("GET /api/v1/files/presign-download", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandlePresignDownload),
	))

	// Admin routes
	mux.Handle("PUT /api/v1/admin/workers/config", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleWorkerConfig),
	))

	mux.Handle("PUT /api/v1/admin/nodes/config", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleConfigureNodes),
	))

	mux.Handle("/api/v1/admin/users", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				h.HandleAdminCreateUser(w, r)
			default:
				httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		}),
	))

	mux.Handle("/api/v1/admin/users/{username}", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodDelete:
				h.HandleAdminDeleteUser(w, r)
			default:
				httputil.WriteErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		}),
	))
}

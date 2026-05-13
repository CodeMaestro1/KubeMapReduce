package api

import (
	"net/http"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/pkg/httputil"
)

// RegisterRoutes wires up the HTTP handlers to their respective URL patterns.
// It applies rate limiting to all user-facing endpoints to prevent abuse.
func RegisterRoutes(mux *http.ServeMux, h *Handlers, validator *auth.JWTValidator) {
	// Create a per-user rate limiter that allows 100 requests per second per user.
	// This can be tuned based on load testing and SLA requirements.
	rateLimiter := PerUserRateLimitMiddleware(100)

	// Public routes
	mux.HandleFunc("/", h.HandleRoot)
	mux.HandleFunc("/healthz", h.HandleHealthz)
	mux.HandleFunc("/readyz", h.HandleReadyz)

	// Authenticated routes - all wrapped with rate limiting
	mux.Handle("/api/v1/jobs", rateLimiter(auth.RequireAnyRole(
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
	)))

	mux.Handle("DELETE /api/v1/jobs/{job_id}", rateLimiter(auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsDelete),
	)))

	mux.Handle("GET /api/v1/jobs/{job_id}", rateLimiter(auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsGet),
	)))

	// File Management - rate limited
	mux.Handle("POST /api/v1/uploads/presigned", rateLimiter(auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandlePresignUpload),
	)))

	mux.Handle("POST /api/v1/downloads/presigned", rateLimiter(auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandlePresignDownload),
	)))

	// Admin routes - rate limited
	mux.Handle("POST /api/v1/admin/config/workers", rateLimiter(auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleAdminConfigWorkers),
	)))

	mux.Handle("GET /api/v1/admin/config/workers", rateLimiter(auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleAdminGetWorkerConfig),
	)))

	mux.Handle("/api/v1/admin/users", rateLimiter(auth.RequireRole(
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
	)))

	mux.Handle("/api/v1/admin/users/{username}", rateLimiter(auth.RequireRole(
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
	)))
}

package api

import (
	"net/http"

	"kubemapreduce/pkg/auth"
)

func RegisterRoutes(mux *http.ServeMux, h *Handlers, validator *auth.JWTValidator) {
	// Public routes
	mux.HandleFunc("/", h.HandleRoot)
	mux.HandleFunc("/health", h.HandleHealth)

	// Authenticated routes
	mux.Handle("/jobs", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				h.HandleJobsSubmit(w, r)
			case http.MethodGet:
				h.HandleJobsList(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}),
	))

	mux.Handle("GET /jobs/{job_id}/results", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsDownload),
	))

	mux.Handle("GET /jobs/{job_id}", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsGet),
	))

	// Admin routes
	mux.Handle("/admin/workers/config", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleWorkerConfig),
	))

	mux.Handle("/admin/users", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				h.HandleAdminCreateUser(w, r)
			case http.MethodDelete:
				h.HandleAdminDeleteUser(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}),
	))
}

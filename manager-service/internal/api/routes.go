package api

import (
	"net/http"

	"kubemapreduce/auth-service/pkg/auth"
)

// RegisterRoutes wires up the HTTP handlers to their respective URL patterns.
//
// It separates routes into public endpoints (root, health) and authenticated 
// endpoints protected by [auth.RequireAnyRole] or [auth.RequireRole] 
// middlewares. The routing uses the standard library's [http.ServeMux] but 
// leverages the 1.22+ pattern matching features (e.g., "GET /jobs/{job_id}").
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

	mux.Handle("/admin/nodes/config", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleConfigureNodes),
	))

	mux.Handle("/admin/users", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				h.HandleAdminCreateUser(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}),
	))

	mux.Handle("/admin/users/{username}", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodDelete:
				h.HandleAdminDeleteUser(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}),
	))
}

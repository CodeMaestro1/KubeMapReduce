package api

import (
	"net/http"
	"strings"

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

	mux.Handle("/jobs/", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/jobs/")
			parts := strings.SplitN(path, "/", 2)
			if len(parts) == 2 && parts[1] == "results" {
				if r.Method != http.MethodGet {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				h.HandleJobsDownload(w, r)
			} else {
				if r.Method != http.MethodGet {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				h.HandleJobsGet(w, r)
			}
		}),
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

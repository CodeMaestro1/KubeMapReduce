package api

import (
	"net/http"

	"kubemapreduce/pkg/auth"
)

func RegisterRoutes(mux *http.ServeMux, h *Handlers, validator *auth.JWTValidator) {
	// Public routes
	mux.HandleFunc("/", h.HandleRoot)
	mux.HandleFunc("/ui/config", h.HandleUIConfig)
	mux.HandleFunc("/health", h.HandleHealth)

	// Authenticated routes
	mux.Handle("/jobs", auth.RequireAnyRole(
		[]string{"USER", "ADMIN"},
		validator,
		http.HandlerFunc(h.HandleJobsSubmit),
	))

	// Admin routes
	mux.Handle("/admin/users", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleCreateUser),
	))

	mux.Handle("/admin/users/", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleDeleteUser),
	))

	mux.Handle("/admin/workers/config", auth.RequireRole(
		"ADMIN",
		validator,
		http.HandlerFunc(h.HandleWorkerConfig),
	))
}

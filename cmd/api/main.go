package main

import (
	"log"
	"net/http"
	"strings"

	"kubemapreduce/internal/api"
	"kubemapreduce/internal/config"
	"kubemapreduce/pkg/auth"
)

func main() {
	cfg := config.Load()

	validator, err := auth.NewJWTValidator(cfg.JWKSURL, cfg.Issuer, cfg.Audience)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): Keycloak returned 404. Ensure realm %q and client %q exist, or override KEYCLOAK_REALM/KEYCLOAK_JWKS_URL/KEYCLOAK_ISSUER. Original error: %v",
				cfg.JWKSURL, cfg.Issuer, cfg.Audience, cfg.Realm, cfg.Audience, err)
		}
		log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): %v",
			cfg.JWKSURL, cfg.Issuer, cfg.Audience, err)
	}

	adminClient := auth.NewKeycloakAdminClient(
		cfg.KeycloakBaseURL,
		cfg.Realm,
		cfg.AdminUsername,
		cfg.AdminPassword,
	)

	handlers := api.NewHandlers(adminClient)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handlers, validator)

	log.Printf("API running on %s", cfg.ServerAddr)
	log.Fatal(http.ListenAndServe(cfg.ServerAddr, mux))
}

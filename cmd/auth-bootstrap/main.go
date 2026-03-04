package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"kubemapreduce/pkg/auth"
)

func main() {
	var cfg auth.BootstrapConfig

	flag.StringVar(&cfg.BaseURL, "keycloak-base-url", getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080"), "Keycloak base URL")
	flag.StringVar(&cfg.Realm, "realm", getEnv("KEYCLOAK_REALM", "mapreduce"), "Target Keycloak realm")
	flag.StringVar(&cfg.ClientID, "client-id", getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api"), "OIDC client id")
	flag.StringVar(&cfg.UIOrigin, "ui-origin", "http://localhost:8081", "UI origin used for redirect URIs and web origins")
	flag.StringVar(&cfg.AdminUsername, "admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username (master realm)")
	flag.StringVar(&cfg.AdminPassword, "admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", "admin"), "Keycloak admin password (master realm)")
	flag.BoolVar(&cfg.EnableRegistration, "enable-registration", true, "Enable self-registration in realm")

	flag.Parse()

	if err := auth.BootstrapKeycloak(cfg, os.Stdout); err != nil {
		log.Fatalf("auth bootstrap failed: %v", err)
	}

	fmt.Println("auth bootstrap completed")
	fmt.Println("note: no users were created by this command")
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"

	"golang.org/x/term"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("setup: ")

	var cfg auth.BootstrapConfig

	// Bootstrap flags
	flag.StringVar(&cfg.BaseURL, "keycloak-base-url", getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080"), "Keycloak base URL")
	flag.StringVar(&cfg.Realm, "realm", getEnv("KEYCLOAK_REALM", "mapreduce"), "Target Keycloak realm")
	flag.StringVar(&cfg.ClientID, "client-id", getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api"), "OIDC client id")
	flag.StringVar(&cfg.UIOrigin, "ui-origin", "http://localhost:8081", "UI origin used for redirect URIs and web origins")
	flag.StringVar(&cfg.AdminUsername, "admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username (master realm)")
	flag.StringVar(&cfg.AdminPassword, "admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", "admin"), "Keycloak admin password (master realm)")
	flag.BoolVar(&cfg.EnableRegistration, "enable-registration", true, "Enable self-registration in realm")

	// Optional: create initial user
	username := flag.String("username", "", "Username to create after bootstrap (optional)")
	email := flag.String("email", "", "Email for the new user")
	password := flag.String("password", "", "Password for the new user")
	promptPassword := flag.Bool("prompt-password", false, "Prompt securely for new user password (input hidden)")
	role := flag.String("role", "ADMIN", "Role to assign: ADMIN or USER")

	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Step 1: Bootstrap realm, client, roles, audience mapper ---
	fmt.Println("==> Bootstrapping Keycloak realm...")
	if err := auth.BootstrapKeycloakWithContext(ctx, cfg, os.Stdout); err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}
	fmt.Println("==> Bootstrap completed.")

	// --- Step 2 (optional): Create initial user ---
	if strings.TrimSpace(*username) == "" {
		fmt.Println("\nNo --username provided, skipping user creation.")
		fmt.Println("Use 'kubemapreduce admin create-user' or re-run this command with --username to create users later.")
		return
	}

	if *promptPassword && strings.TrimSpace(*password) != "" {
		log.Fatal("use either --password or --prompt-password, not both")
	}

	userPassword := strings.TrimSpace(*password)
	if *promptPassword {
		fmt.Fprint(os.Stderr, "Enter password for new user: ")
		rawPassword, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			log.Fatalf("failed to read password from prompt: %v", err)
		}
		userPassword = strings.TrimSpace(string(rawPassword))
	}

	if userPassword == "" {
		log.Fatal("--username was provided but no password: use --password or --prompt-password")
	}

	normalizedRole := strings.ToUpper(strings.TrimSpace(*role))
	if normalizedRole != "ADMIN" && normalizedRole != "USER" {
		log.Fatal("invalid --role, expected ADMIN or USER")
	}

	fmt.Printf("==> Creating %s user %q...\n", normalizedRole, strings.TrimSpace(*username))

	client := auth.NewKeycloakAdminClient(
		strings.TrimSpace(cfg.BaseURL),
		strings.TrimSpace(cfg.Realm),
		strings.TrimSpace(cfg.AdminUsername),
		strings.TrimSpace(cfg.AdminPassword),
	)

	if err := client.CreateUser(ctx, auth.CreateUserRequest{
		Username: strings.TrimSpace(*username),
		Email:    strings.TrimSpace(*email),
		Password: userPassword,
		Role:     normalizedRole,
	}); err != nil {
		log.Fatalf("failed to create user: %v", err)
	}

	fmt.Printf("==> Created %s user %q in realm %q.\n", normalizedRole, strings.TrimSpace(*username), cfg.Realm)
	fmt.Println("\nSetup complete! You can now start the API server and log in via the CLI.")
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

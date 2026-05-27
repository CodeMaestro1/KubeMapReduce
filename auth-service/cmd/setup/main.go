package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/pkg/observability"

	"golang.org/x/term"
)

// main bootstraps the Keycloak environment for KubeMapReduce.
//
// The bootstrapping sequence follows these steps:
//  1. Parse command-line flags and environment variables for Keycloak connection and realm settings.
//  2. Initialize a context with a 60-second timeout to bound the setup process.
//  3. Call [auth.BootstrapKeycloakWithContext] to create the realm, OIDC client, roles (ADMIN/USER), and audience mappers.
//  4. (Optional) If a username is provided via flags, create an initial user with the specified role and password.
//  5. Exit upon completion or failure.
func main() {
	logger := observability.NewLogger("auth-setup")
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(loggerWriter{logger: logger})

	var cfg auth.BootstrapConfig

	// Bootstrap flags
	flag.StringVar(&cfg.BaseURL, "keycloak-base-url", getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080"), "Keycloak base URL")
	flag.StringVar(&cfg.Realm, "realm", getEnv("KEYCLOAK_REALM", "mapreduce"), "Target Keycloak realm")
	flag.StringVar(&cfg.ClientID, "client-id", getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api"), "OIDC client id")
	flag.StringVar(&cfg.UIOrigin, "ui-origin", "http://localhost:8081", "UI origin used for redirect URIs and web origins")
	flag.StringVar(&cfg.AdminUsername, "admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username (master realm)")
	flag.StringVar(&cfg.AdminPassword, "admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", ""), "Keycloak admin password (master realm)")
	flag.BoolVar(&cfg.EnableRegistration, "enable-registration", false, "Enable self-registration in realm")

	// Optional: create initial user
	username := flag.String("username", "", "Username to create after bootstrap (optional)")
	email := flag.String("email", "", "Email for the new user")
	password := flag.String("password", "", "Password for the new user")
	promptPassword := flag.Bool("prompt-password", false, "Prompt securely for new user password (input hidden)")
	role := flag.String("role", "ADMIN", "Role to assign: ADMIN or USER")

	flag.Parse()

	if strings.TrimSpace(cfg.AdminPassword) == "" {
		log.Fatal("KEYCLOAK_ADMIN_PASSWORD must be set; refusing to bootstrap with an empty admin password")
	}

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
		if strings.Contains(err.Error(), "status 409") {
			fmt.Printf("==> User %q already exists, skipping user creation.\n", strings.TrimSpace(*username))
		} else {
			log.Fatalf("failed to create user: %v", err)
		}
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

// loggerWriter bridges log.Print* output to the structured slog pipeline so
// every line emitted by the standard logger (including log.Fatalf at
// shutdown) is routed through the same JSON sink as the rest of the
// process.
type loggerWriter struct {
	logger *slog.Logger
}

func (w loggerWriter) Write(p []byte) (int, error) {
	msg := string(p)
	if n := len(msg); n > 0 && msg[n-1] == '\n' {
		msg = msg[:n-1]
	}
	w.logger.Info(msg)
	return len(p), nil
}

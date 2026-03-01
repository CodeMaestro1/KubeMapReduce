package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"kubemapreduce/pkg/auth"

	"golang.org/x/term"
)

func main() {
	var (
		keycloakBaseURL = flag.String("keycloak-base-url", getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080"), "Keycloak base URL")
		realm           = flag.String("realm", getEnv("KEYCLOAK_REALM", "mapreduce"), "Target Keycloak realm")
		adminUsername   = flag.String("admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", ""), "Keycloak admin username (master realm)")
		adminPassword   = flag.String("admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", ""), "Keycloak admin password (master realm)")
		username        = flag.String("username", "", "Username to create")
		email           = flag.String("email", "", "Email for the new user")
		password        = flag.String("password", "", "Password for the new user")
		promptPassword  = flag.Bool("prompt-password", false, "Prompt securely for new user password (input hidden)")
		role            = flag.String("role", "ADMIN", "Role to assign: ADMIN or USER")
	)

	flag.Parse()

	if strings.TrimSpace(*adminUsername) == "" || strings.TrimSpace(*adminPassword) == "" {
		log.Fatal("missing Keycloak admin credentials: provide --admin-username/--admin-password or set KEYCLOAK_ADMIN_USERNAME/KEYCLOAK_ADMIN_PASSWORD")
	}

	if strings.TrimSpace(*username) == "" {
		log.Fatal("missing --username")
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
		log.Fatal("missing user password: provide --password or use --prompt-password")
	}

	normalizedRole := strings.ToUpper(strings.TrimSpace(*role))
	if normalizedRole != "ADMIN" && normalizedRole != "USER" {
		log.Fatal("invalid --role, expected ADMIN or USER")
	}

	client := auth.NewKeycloakAdminClient(
		strings.TrimSpace(*keycloakBaseURL),
		strings.TrimSpace(*realm),
		strings.TrimSpace(*adminUsername),
		strings.TrimSpace(*adminPassword),
	)

	if err := client.CreateUser(auth.CreateUserRequest{
		Username: strings.TrimSpace(*username),
		Email:    strings.TrimSpace(*email),
		Password: userPassword,
		Role:     normalizedRole,
	}); err != nil {
		log.Fatalf("failed to create %s user %q in realm %q: %v", normalizedRole, strings.TrimSpace(*username), strings.TrimSpace(*realm), err)
	}

	fmt.Printf("success: created %s user %q in realm %q\n", normalizedRole, strings.TrimSpace(*username), strings.TrimSpace(*realm))
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

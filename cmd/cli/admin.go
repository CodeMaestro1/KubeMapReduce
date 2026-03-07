package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"kubemapreduce/pkg/auth"

	"golang.org/x/term"
)

// ── admin helpers ──────────────────────────────────────────

// requireAdminRole checks that the logged-in user has the ADMIN realm role.
func requireAdminRole() {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatal("not logged in — run 'kubemapreduce login' first")
	}

	claims, err := decodeTokenClaims(tokens.AccessToken)
	if err != nil {
		log.Fatalf("failed to read token: %v", err)
	}

	if ra, ok := claims["realm_access"].(map[string]any); ok {
		if roles, ok := ra["roles"].([]any); ok {
			for _, r := range roles {
				if s, ok := r.(string); ok && s == "ADMIN" {
					return
				}
			}
		}
	}

	username, _ := claims["preferred_username"].(string)
	log.Fatalf("permission denied: user %q does not have the ADMIN role", username)
}

// ── admin create-user ──────────────────────────────────────
// Routes through the API server, which proxies to Keycloak.

func cmdAdminCreateUser(args []string) {
	requireAdminRole()
	fs := flag.NewFlagSet("admin create-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to create (required)")
	email := fs.String("email", "", "Email for the new user")
	password := fs.String("password", "", "Password for the new user")
	promptPw := fs.Bool("prompt-password", false, "Prompt for password (hidden input)")
	role := fs.String("role", "USER", "Role to assign: ADMIN or USER")
	_ = fs.Parse(args)

	if *username == "" {
		log.Fatal("--username is required")
	}
	if *promptPw && *password != "" {
		log.Fatal("use either --password or --prompt-password, not both")
	}

	normalizedRole := strings.ToUpper(strings.TrimSpace(*role))
	if normalizedRole != "ADMIN" && normalizedRole != "USER" {
		log.Fatal("--role must be ADMIN or USER")
	}

	userPw := strings.TrimSpace(*password)
	if *promptPw {
		fmt.Print("Enter password for new user: ")
		raw, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			log.Fatalf("failed to read password: %v", err)
		}
		userPw = strings.TrimSpace(string(raw))
	}
	if userPw == "" {
		log.Fatal("password is required: use --password or --prompt-password")
	}

	payload, _ := json.Marshal(map[string]string{
		"username": strings.TrimSpace(*username),
		"email":    strings.TrimSpace(*email),
		"password": userPw,
		"role":     normalizedRole,
	})

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodPost, serverURL+"/admin/users", token, payload)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("create user failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Created %s user %q.\n", normalizedRole, *username)
	printResponse(resp)
}

// ── admin delete-user ──────────────────────────────────────
// Routes through the API server, which proxies to Keycloak.

func cmdAdminDeleteUser(args []string) {
	requireAdminRole()
	fs := flag.NewFlagSet("admin delete-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to delete (required)")
	_ = fs.Parse(args)

	if *username == "" {
		log.Fatal("--username is required")
	}

	payload, _ := json.Marshal(map[string]string{
		"username": strings.TrimSpace(*username),
	})

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodDelete, serverURL+"/admin/users", token, payload)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("delete user failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("User %q deleted.\n", *username)
	printResponse(resp)
}

// ── admin worker-config ────────────────────────────────────

func cmdAdminWorkerConfig(args []string) {
	requireAdminRole()
	fs := flag.NewFlagSet("admin worker-config", flag.ExitOnError)
	replicas := fs.Int("replicas", 0, "Number of worker replicas (required, > 0)")
	maxJobs := fs.Int("max-jobs", 0, "Max jobs per node (required, > 0)")
	_ = fs.Parse(args)

	if *replicas < 1 || *maxJobs < 1 {
		log.Fatal("--replicas and --max-jobs are required and must be > 0")
	}

	payload, _ := json.Marshal(map[string]int{
		"workerReplicas": *replicas,
		"maxJobsPerNode": *maxJobs,
	})

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodPut, serverURL+"/admin/workers/config", token, payload)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("worker config update failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	printResponse(resp)
}

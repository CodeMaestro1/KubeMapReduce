package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/term"
)

// ── admin helpers ──────────────────────────────────────────

// requireAdminRole checks that the supplied access token carries the ADMIN realm role.
// The caller should obtain the token via getValidToken() to avoid a redundant disk read.
func requireAdminRole(accessToken string) {
	claims, err := decodeTokenClaims(accessToken)
	if err != nil {
		log.Fatalf("failed to read token: %v", err)
	}

	if hasRealmRole(claims, "ADMIN") {
		return
	}

	username, _ := claims["preferred_username"].(string)
	log.Fatalf("permission denied: user %q does not have the ADMIN role", username)
}

// ── admin create-user ──────────────────────────────────────
// Routes through the API server, which proxies to Keycloak.

func cmdAdminCreateUser(args []string) {
	token, serverURL := getValidToken()
	requireAdminRole(token)
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

	resp := doAuthRequestExpect(
		http.MethodPost,
		serverURL+"/admin/users",
		token,
		payload,
		http.StatusCreated,
		"create user failed",
	)
	defer resp.Body.Close()

	fmt.Printf("Created %s user %q.\n", normalizedRole, *username)
	printResponse(resp)
}

// ── admin delete-user ──────────────────────────────────────
// Routes through the API server, which proxies to Keycloak.

func cmdAdminDeleteUser(args []string) {
	token, serverURL := getValidToken()
	requireAdminRole(token)
	fs := flag.NewFlagSet("admin delete-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to delete (required)")
	_ = fs.Parse(args)

	if *username == "" {
		log.Fatal("--username is required")
	}

	payload, _ := json.Marshal(map[string]string{
		"username": strings.TrimSpace(*username),
	})

	resp := doAuthRequestExpect(
		http.MethodDelete,
		serverURL+"/admin/users",
		token,
		payload,
		http.StatusOK,
		"delete user failed",
	)
	defer resp.Body.Close()

	fmt.Printf("User %q deleted.\n", *username)
	printResponse(resp)
}

// ── admin configure-nodes ─────────────────────────────────

func cmdAdminConfigureNodes(args []string) {
	token, serverURL := getValidToken()
	requireAdminRole(token)
	fs := flag.NewFlagSet("admin configure-nodes", flag.ExitOnError)
	maxPods := fs.Int("max-pods", 0, "Maximum pods per node (required, > 0)")
	cpuLimit := fs.String("cpu-limit", "", "CPU limit per pod, e.g. 500m (required)")
	memoryLimit := fs.String("memory-limit", "", "Memory limit per pod, e.g. 1Gi (required)")
	_ = fs.Parse(args)

	if *maxPods < 1 {
		log.Fatal("--max-pods is required and must be > 0")
	}
	if strings.TrimSpace(*cpuLimit) == "" {
		log.Fatal("--cpu-limit is required")
	}
	if strings.TrimSpace(*memoryLimit) == "" {
		log.Fatal("--memory-limit is required")
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"maxPods":     *maxPods,
		"cpuLimit":    strings.TrimSpace(*cpuLimit),
		"memoryLimit": strings.TrimSpace(*memoryLimit),
	})

	resp := doAuthRequestExpect(
		http.MethodPut,
		serverURL+"/admin/nodes/config",
		token,
		payload,
		http.StatusNotImplemented,
		"configure nodes failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}

// ── admin worker-config ────────────────────────────────────

func cmdAdminWorkerConfig(args []string) {
	token, serverURL := getValidToken()
	requireAdminRole(token)
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

	resp := doAuthRequestExpect(
		http.MethodPut,
		serverURL+"/admin/workers/config",
		token,
		payload,
		http.StatusNotImplemented,
		"worker config update failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}

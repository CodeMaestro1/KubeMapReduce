package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/term"
)

var adminConfigureNodesDoAuthRequest = doAuthRequest
var adminConfigureNodesExit = os.Exit
var adminGetValidToken = getValidToken
var adminRequireAdminRole = requireAdminRole

// ── admin helpers ──────────────────────────────────────────

// requireAdminRole checks that the supplied access token carries the ADMIN realm role.
//
// The caller should obtain the token via [getValidToken] to avoid a redundant disk read.
// This check is performed client-side to provide immediate feedback to the user,
// although the API server will also perform its own validation to ensure security.
func requireAdminRole(accessToken string) {
	claims, err := decodeTokenClaims(accessToken)
	if err != nil {
		log.Fatalf("failed to read token: %v", err)
	}

	if hasAdminRole(claims) {
		return
	}

	username, _ := claims["preferred_username"].(string)
	log.Fatalf("permission denied: user %q does not have the ADMIN role", username)
}

// ── admin create-user ──────────────────────────────────────
// Routes through the API server, which proxies to Keycloak.

// cmdAdminCreateUser creates a new user in the system.
//
// This command routes through the API server, which in turn proxies the request
// to Keycloak. It handles password prompting and role assignment (ADMIN or USER),
// ensuring that administrative actions are centralized and audited.
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

	payload, err := json.Marshal(map[string]string{
		"username": strings.TrimSpace(*username),
		"email":    strings.TrimSpace(*email),
		"password": userPw,
		"role":     normalizedRole,
	})
	if err != nil {
		// Fail fast on serialization errors so we never send a malformed admin payload.
		log.Fatalf("failed to build create-user request: %v", err)
	}

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

// cmdAdminDeleteUser removes a user from the system.
//
// Like user creation, this command proxies through the API server to Keycloak.
// It requires the ADMIN role to prevent unauthorized user management.
func cmdAdminDeleteUser(args []string) {
	token, serverURL := getValidToken()
	requireAdminRole(token)
	fs := flag.NewFlagSet("admin delete-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to delete (required)")
	_ = fs.Parse(args)

	normalizedUsername := strings.TrimSpace(*username)
	if normalizedUsername == "" {
		log.Fatal("--username is required")
	}

	resp := doAuthRequestExpect(
		http.MethodDelete,
		serverURL+"/admin/users/"+url.PathEscape(normalizedUsername),
		token,
		nil,
		http.StatusNoContent,
		"delete user failed",
	)
	defer resp.Body.Close()

	fmt.Printf("User %q deleted.\n", normalizedUsername)
}

// ── admin configure-nodes ─────────────────────────────────

// cmdAdminConfigureNodes updates resource limits across all compute nodes.
//
// This command allows administrators to dynamically adjust the capacity of the
// MapReduce cluster. Changes are propagated to the Manager service, which
// updates its internal scheduling constraints to match the new limits.
func cmdAdminConfigureNodes(args []string) {
	if err := runAdminConfigureNodes(args, adminConfigureNodesDoAuthRequest); err != nil {
		fmt.Fprintln(os.Stderr, err)
		adminConfigureNodesExit(1)
	}
}

// ── admin configure-nodes helpers ─────────────────────────

// runAdminConfigureNodes is the implementation of the admin configure-nodes command.
// It accepts optional doAuthRequest override for testing purposes.
func runAdminConfigureNodes(args []string, doAuthReq func(method, url string, bearerToken string, body []byte) (*http.Response, error)) error {
	token, serverURL := adminGetValidToken()
	adminRequireAdminRole(token)
	fs := flag.NewFlagSet("admin configure-nodes", flag.ExitOnError)
	maxPods := fs.Int("max-pods", 0, "Maximum concurrent pods (required, > 0)")
	cpuLimit := fs.String("cpu", "", "CPU limit per worker pod (e.g., 500m)")
	memoryLimit := fs.String("memory", "", "Memory limit per worker pod (e.g., 1Gi)")
	_ = fs.Parse(args)

	if *maxPods < 1 {
		return fmt.Errorf("--max-pods is required and must be > 0")
	}

	payload, err := json.Marshal(map[string]interface{}{
		"maxConcurrentPods": *maxPods,
		"cpuLimit":          strings.TrimSpace(*cpuLimit),
		"memoryLimit":       strings.TrimSpace(*memoryLimit),
	})
	if err != nil {
		return fmt.Errorf("failed to build configure-nodes request: %v", err)
	}

	resp, err := doAuthReq(http.MethodPut, serverURL+"/admin/nodes/config", token, payload)
	if err != nil {
		return fmt.Errorf("configure-nodes request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if statusErr := configureNodesStatusError(resp.StatusCode, string(body)); statusErr != nil {
		return statusErr
	}

	fmt.Printf("Node configuration updated successfully.\n")
	return nil
}

// configureNodesStatusError evaluates the HTTP response status from a configure-nodes request
// and returns an appropriate error, or nil if the response was successful.
//
// HTTP 202 (Accepted) is treated as a success and returns nil.
// HTTP 501 (Not Implemented) returns a helpful error mentioning backend integration status.
// Other HTTP statuses return an error including the status code and response body.
func configureNodesStatusError(status int, body string) error {
	if status == http.StatusAccepted {
		return nil
	}

	if status == http.StatusNotImplemented {
		return fmt.Errorf("HTTP 501 Not Implemented: node configuration backend integration is not implemented yet (pending backend implementation)")
	}

	return fmt.Errorf("HTTP %d: %s", status, body)
}

// ── admin worker-config ────────────────────────────────────

// cmdAdminWorkerConfig updates the configuration for worker pods.
//
// This command controls the scale and density of the worker fleet. By adjusting
// the number of replicas and the maximum number of concurrent jobs per node,
// administrators can tune the system's performance and resource utilization
// based on current workload demands.
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

	payload, err := json.Marshal(map[string]int{
		"workerReplicas": *replicas,
		"maxJobsPerNode": *maxJobs,
	})
	if err != nil {
		// Defensive check: avoid issuing requests with partially built JSON bodies.
		log.Fatalf("failed to build worker-config request: %v", err)
	}

	resp := doAuthRequestExpect(
		http.MethodPut,
		serverURL+"/admin/workers/config",
		token,
		payload,
		http.StatusAccepted,
		"worker config update failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}

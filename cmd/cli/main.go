package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"kubemapreduce/pkg/auth"

	"golang.org/x/term"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("kubemapreduce: ")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "login":
		cmdLogin(os.Args[2:])
	case "logout":
		cmdLogout()
	case "health":
		cmdHealth()
	case "jobs":
		if len(os.Args) < 3 || os.Args[2] != "submit" {
			log.Fatal("usage: kubemapreduce jobs submit [flags] <file.json>")
		}
		cmdJobsSubmit(os.Args[3:])
	case "admin":
		if len(os.Args) < 3 {
			log.Fatal("usage: kubemapreduce admin <create-user|delete-user|worker-config> [flags]")
		}
		switch os.Args[2] {
		case "create-user":
			cmdAdminCreateUser(os.Args[3:])
		case "delete-user":
			cmdAdminDeleteUser(os.Args[3:])
		case "worker-config":
			cmdAdminWorkerConfig(os.Args[3:])
		default:
			log.Fatalf("unknown admin subcommand: %s", os.Args[2])
		}
	case "whoami":
		cmdWhoAmI()
	case "token":
		if len(os.Args) >= 3 && os.Args[2] == "inspect" {
			cmdTokenInspect()
		} else {
			log.Fatal("usage: kubemapreduce token inspect")
		}
	case "help", "--help", "-h":
		printUsage()
	default:
		log.Fatalf("unknown command: %s\nRun 'kubemapreduce help' for usage.", os.Args[1])
	}
}

func printUsage() {
	fmt.Print(`KubeMapReduce CLI

Usage:
  kubemapreduce <command> [flags]

Commands:
  login                  Authenticate with Keycloak and store tokens
  logout                 Clear stored authentication tokens
  health                 Check API server health
  jobs submit <file>     Submit a MapReduce job specification (use "-" for stdin)
  whoami                 Show the currently logged-in user
  admin create-user      Create a user in Keycloak (ADMIN)
  admin delete-user      Delete a user from Keycloak (ADMIN)
  admin worker-config    Update worker configuration (ADMIN)
  token inspect          Show raw JWT claims for the stored access token
  help                   Show this help message

Environment Variables:
  API_URL                API server URL          (default: http://localhost:8081)
  KEYCLOAK_BASE_URL      Keycloak base URL       (default: http://localhost:8080)
  KEYCLOAK_REALM         Keycloak realm          (default: mapreduce)
  KEYCLOAK_AUDIENCE      Keycloak client ID      (default: mapreduce-api)
`)
}

// ── environment helpers ────────────────────────────────────

func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func apiURL() string           { return getEnv("API_URL", "http://localhost:8081") }
func keycloakBaseURL() string  { return getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080") }
func keycloakRealm() string    { return getEnv("KEYCLOAK_REALM", "mapreduce") }
func keycloakClientID() string { return getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api") }

// ── login ──────────────────────────────────────────────────

func cmdLogin(args []string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	username := fs.String("username", "", "Username (prompted if empty)")
	_ = fs.Parse(args)

	u := strings.TrimSpace(*username)
	if u == "" {
		fmt.Print("Username: ")
		fmt.Scanln(&u)
		u = strings.TrimSpace(u)
	}
	if u == "" {
		log.Fatal("username is required")
	}

	fmt.Print("Password: ")
	rawPw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		log.Fatalf("failed to read password: %v", err)
	}
	pw := strings.TrimSpace(string(rawPw))
	if pw == "" {
		log.Fatal("password is required")
	}

	tokenResp, err := auth.RequestTokens(keycloakBaseURL(), keycloakRealm(), keycloakClientID(), u, pw)
	if err != nil {
		log.Fatalf("login failed: %v", err)
	}

	if err := auth.SaveTokens(&auth.StoredTokens{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAt:    time.Now().Unix() + int64(tokenResp.ExpiresIn),
		ServerURL:    apiURL(),
	}); err != nil {
		log.Fatalf("failed to save credentials: %v", err)
	}

	path, _ := auth.TokenStorePath()
	fmt.Println("Login successful!")
	fmt.Printf("Credentials saved to %s\n", path)
}

// ── logout ─────────────────────────────────────────────────

func cmdLogout() {
	if err := auth.ClearTokens(); err != nil {
		log.Fatalf("logout failed: %v", err)
	}
	fmt.Println("Logged out.")
}

// ── token inspect ──────────────────────────────────────────

func cmdWhoAmI() {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatal("not logged in — run 'kubemapreduce login' first")
	}

	claims, err := decodeTokenClaims(tokens.AccessToken)
	if err != nil {
		log.Fatalf("failed to read token: %v", err)
	}

	username, _ := claims["preferred_username"].(string)
	email, _ := claims["email"].(string)
	sub, _ := claims["sub"].(string)

	var roles []string
	if ra, ok := claims["realm_access"].(map[string]any); ok {
		if r, ok := ra["roles"].([]any); ok {
			for _, v := range r {
				if s, ok := v.(string); ok {
					roles = append(roles, s)
				}
			}
		}
	}

	fmt.Printf("Username: %s\n", username)
	if email != "" {
		fmt.Printf("Email:    %s\n", email)
	}
	fmt.Printf("Subject:  %s\n", sub)
	if len(roles) > 0 {
		fmt.Printf("Roles:    %s\n", strings.Join(roles, ", "))
	}

	if tokens.IsAccessExpired() {
		fmt.Println("\nAccess token is expired — it will be refreshed on the next API call.")
	}
}

// decodeTokenClaims decodes the JWT payload without verification.
func decodeTokenClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("not a valid JWT")
	}

	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}

	var claims map[string]any
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func cmdTokenInspect() {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatalf("%v", err)
	}

	claims, err := decodeTokenClaims(tokens.AccessToken)
	if err != nil {
		log.Fatalf("failed to decode token: %v", err)
	}

	formatted, _ := json.MarshalIndent(claims, "", "  ")
	fmt.Println(string(formatted))

	// Print a quick summary of key fields.
	fmt.Println("\n--- Summary ---")
	fmt.Printf("  iss: %v\n", claims["iss"])
	fmt.Printf("  aud: %v\n", claims["aud"])
	fmt.Printf("  sub: %v\n", claims["sub"])
	fmt.Printf("  preferred_username: %v\n", claims["preferred_username"])
	if ra, ok := claims["realm_access"].(map[string]any); ok {
		fmt.Printf("  realm_access.roles: %v\n", ra["roles"])
	}

	if tokens.IsAccessExpired() {
		fmt.Println("  ⚠ access token is EXPIRED")
	} else {
		fmt.Println("  ✓ access token is valid")
	}
}

// ── health ─────────────────────────────────────────────────

func cmdHealth() {
	resp, err := http.Get(apiURL() + "/health")
	if err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
}

// ── token management ───────────────────────────────────────

func getValidToken() (token string, serverURL string) {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatalf("%v\nRun 'kubemapreduce login' first.", err)
	}

	if !tokens.IsAccessExpired() {
		return tokens.AccessToken, tokens.ServerURL
	}

	// Access token expired — try refreshing with the refresh token.
	tokenResp, err := auth.RefreshTokens(
		keycloakBaseURL(), keycloakRealm(), keycloakClientID(), tokens.RefreshToken,
	)
	if err != nil {
		log.Fatalf("session expired, please login again: %v", err)
	}

	tokens.AccessToken = tokenResp.AccessToken
	tokens.RefreshToken = tokenResp.RefreshToken
	tokens.ExpiresAt = time.Now().Unix() + int64(tokenResp.ExpiresIn)

	if err := auth.SaveTokens(tokens); err != nil {
		log.Fatalf("failed to update credentials: %v", err)
	}

	return tokens.AccessToken, tokens.ServerURL
}

func doAuthRequest(method, reqURL, token string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, reqURL, reader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return http.DefaultClient.Do(req)
}

func printResponse(resp *http.Response) {
	body, _ := io.ReadAll(resp.Body)

	var buf bytes.Buffer
	if json.Indent(&buf, body, "", "  ") == nil {
		fmt.Println(buf.String())
	} else {
		fmt.Print(string(body))
	}
}

// ── jobs submit ────────────────────────────────────────────

func cmdJobsSubmit(args []string) {
	fs := flag.NewFlagSet("jobs submit", flag.ExitOnError)
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		log.Fatal("usage: kubemapreduce jobs submit <file.json>  (use \"-\" for stdin)")
	}

	filename := fs.Arg(0)
	var data []byte
	var err error

	if filename == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(filename)
	}
	if err != nil {
		log.Fatalf("failed to read input: %v", err)
	}

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodPost, serverURL+"/jobs", token, data)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("job submission failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	printResponse(resp)
}

// ── admin helpers ──────────────────────────────────────────

// requireAdminRole checks that the logged-in user has the ADMIN realm role.
func requireAdminRole() {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatal("not logged in \u2014 run 'kubemapreduce login' first")
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
// Talks directly to Keycloak — no API server round-trip needed.

func cmdAdminCreateUser(args []string) {
	requireAdminRole()
	fs := flag.NewFlagSet("admin create-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to create (required)")
	email := fs.String("email", "", "Email for the new user")
	password := fs.String("password", "", "Password for the new user")
	promptPw := fs.Bool("prompt-password", false, "Prompt for password (hidden input)")
	role := fs.String("role", "USER", "Role to assign: ADMIN or USER")
	adminUser := fs.String("admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username")
	adminPass := fs.String("admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", "admin"), "Keycloak admin password")
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

	client := auth.NewKeycloakAdminClient(
		keycloakBaseURL(),
		keycloakRealm(),
		strings.TrimSpace(*adminUser),
		strings.TrimSpace(*adminPass),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.CreateUser(ctx, auth.CreateUserRequest{
		Username: strings.TrimSpace(*username),
		Email:    strings.TrimSpace(*email),
		Password: userPw,
		Role:     normalizedRole,
	}); err != nil {
		log.Fatalf("failed to create user: %v", err)
	}

	fmt.Printf("Created %s user %q in realm %q.\n", normalizedRole, *username, keycloakRealm())
}

// ── admin delete-user ──────────────────────────────────────
// Talks directly to Keycloak — no API server round-trip needed.

func cmdAdminDeleteUser(args []string) {
	requireAdminRole()
	fs := flag.NewFlagSet("admin delete-user", flag.ExitOnError)
	username := fs.String("username", "", "Username to delete (required)")
	adminUser := fs.String("admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username")
	adminPass := fs.String("admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", "admin"), "Keycloak admin password")
	_ = fs.Parse(args)

	if *username == "" {
		log.Fatal("--username is required")
	}

	client := auth.NewKeycloakAdminClient(
		keycloakBaseURL(),
		keycloakRealm(),
		strings.TrimSpace(*adminUser),
		strings.TrimSpace(*adminPass),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.DeleteUserByUsername(ctx, strings.TrimSpace(*username)); err != nil {
		log.Fatalf("failed to delete user: %v", err)
	}

	fmt.Printf("User %q deleted from realm %q.\n", *username, keycloakRealm())
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

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"

	"golang.org/x/term"
)

// ── login ──────────────────────────────────────────────────

// cmdLogin authenticates a user against Keycloak and stores the resulting tokens.
//
// It prompts for a username (if not provided) and password, then exchanges these
// credentials for a JWT access token and refresh token. These tokens are persisted
// to a local configuration file (typically in the user's home directory),
// enabling subsequent commands to be authenticated without further user interaction.
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

	ctx, cancel := cliRequestContext()
	defer cancel()

	tokenResp, err := auth.RequestTokensWithContext(
		ctx,
		cliHTTPClient,
		keycloakBaseURL(),
		keycloakRealm(),
		keycloakClientID(),
		u,
		pw,
	)
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

// cmdLogout clears the stored authentication tokens from the local system.
//
// This effectively logs the user out of the CLI by removing the credentials
// that [getValidToken] would otherwise use to authenticate requests.
func cmdLogout() {
	if err := auth.ClearTokens(); err != nil {
		log.Fatalf("logout failed: %v", err)
	}
	fmt.Println("Logged out.")
}

// ── health ─────────────────────────────────────────────────

// cmdHealth performs a connectivity and status check against the API server.
//
// This command is used to verify that the CLI can communicate with the backend
// and that the backend itself is operational. It does not require authentication,
// making it a useful first-step diagnostic tool.
func cmdHealth() {
	ctx, cancel := cliRequestContext()
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL()+"/health", nil)
	if err != nil {
		log.Fatalf("health check request failed: %v", err)
	}

	resp, err := cliHTTPClient.Do(req)
	if err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := readResponseBody(resp.Body)
	if err != nil {
		log.Fatalf("health check failed while reading response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("health check failed (HTTP %s): %s", resp.Status, strings.TrimSpace(string(body)))
	}

	fmt.Print(string(body))
}

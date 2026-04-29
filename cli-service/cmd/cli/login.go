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

var readPasswordFn = term.ReadPassword

// ── login ──────────────────────────────────────────────────

// cmdLogin authenticates a user against Keycloak and stores the resulting tokens.
//
// It prompts for a username (if not provided) and password, then exchanges these
// credentials for a JWT access token and refresh token. These tokens are persisted
// to a local configuration file (typically in the user's home directory),
// enabling subsequent commands to be authenticated without further user interaction.
func cmdLogin(args []string) {
	if err := runLogin(args); err != nil {
		log.Fatal(err)
	}
}

func runLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	username := fs.String("username", "", "Username (prompted if empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	u := strings.TrimSpace(*username)
	if u == "" {
		fmt.Print("Username: ")
		fmt.Scanln(&u)
		u = strings.TrimSpace(u)
	}
	if u == "" {
		return fmt.Errorf("username is required")
	}

	fmt.Print("Password: ")
	rawPw, err := readPasswordFn(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return fmt.Errorf("failed to read password: %w", err)
	}
	pw := strings.TrimSpace(string(rawPw))
	if pw == "" {
		return fmt.Errorf("password is required")
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
		return fmt.Errorf("login failed: %w", err)
	}

	if err := auth.SaveTokens(&auth.StoredTokens{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAt:    time.Now().Unix() + int64(tokenResp.ExpiresIn),
		ServerURL:    apiURL(),
	}); err != nil {
		return fmt.Errorf("failed to save credentials: %w", err)
	}

	path, _ := auth.TokenStorePath()
	fmt.Println("Login successful!")
	fmt.Printf("Credentials saved to %s\n", path)
	return nil
}

// ── logout ─────────────────────────────────────────────────

// cmdLogout clears the stored authentication tokens from the local system.
//
// This effectively logs the user out of the CLI by removing the credentials
// that [getValidToken] would otherwise use to authenticate requests.
func cmdLogout() {
	if err := runLogout(); err != nil {
		log.Fatal(err)
	}
}

func runLogout() error {
	if err := auth.ClearTokens(); err != nil {
		return fmt.Errorf("logout failed: %w", err)
	}
	fmt.Println("Logged out.")
	return nil
}

// ── health ─────────────────────────────────────────────────

// cmdHealth performs a connectivity and status check against the API server.
//
// This command is used to verify that the CLI can communicate with the backend
// and that the backend itself is operational. It does not require authentication,
// making it a useful first-step diagnostic tool.
func cmdHealth() {
	if err := runHealth(); err != nil {
		log.Fatal(err)
	}
}

func runHealth() error {
	ctx, cancel := cliRequestContext()
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL()+"/health", nil)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}

	resp, err := cliHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := readResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("health check failed while reading response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed (HTTP %s): %s", resp.Status, strings.TrimSpace(string(body)))
	}

	fmt.Print(string(body))
	return nil
}

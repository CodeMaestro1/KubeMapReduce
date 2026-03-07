package main

import (
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

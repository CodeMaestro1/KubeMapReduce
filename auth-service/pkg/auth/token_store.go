package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// StoredTokens represents the token state persisted to the local filesystem.
//
// This allows the CLI to remain authenticated across different command
// invocations. The refresh token is used to obtain new access tokens when the
// current one expires, reducing the frequency of interactive logins.
type StoredTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ServerURL    string `json:"server_url"`
}

// IsAccessExpired checks if the access token has expired.
//
// It returns true if the token is already expired or if it will expire
// within a 30-second buffer. This buffer prevents race conditions between the
// CLI check and the Manager's validation.
func (t *StoredTokens) IsAccessExpired() bool {
	return time.Now().Unix() >= t.ExpiresAt-30
}

// TokenStorePath returns the platform-appropriate path for the credentials file.
//
// On Windows, it uses %APPDATA%\kubemapreduce\credentials.json.
// On Unix-like systems, it follows the XDG Base Directory Specification,
// defaulting to ~/.config/kubemapreduce/credentials.json.
func TokenStorePath() (string, error) {
	var configDir string

	if runtime.GOOS == "windows" {
		configDir = os.Getenv("APPDATA")
		if configDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("cannot determine home directory: %w", err)
			}
			configDir = filepath.Join(home, "AppData", "Roaming")
		}
	} else {
		configDir = os.Getenv("XDG_CONFIG_HOME")
		if configDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("cannot determine home directory: %w", err)
			}
			configDir = filepath.Join(home, ".config")
		}
	}

	return filepath.Join(configDir, "kubemapreduce", "credentials.json"), nil
}

// SaveTokens persists [StoredTokens] to the credentials file with restricted
// (0600) permissions.
//
// Restricted permissions are critical as the file contains long-lived
// refresh tokens.
func SaveTokens(tokens *StoredTokens) error {
	path, err := TokenStorePath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tokens: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write credentials file: %w", err)
	}

	return nil
}

// LoadTokens reads the stored tokens from disk.
//
// If the file is missing, it returns a descriptive error instructing the
// user to login.
func LoadTokens() (*StoredTokens, error) {
	path, err := TokenStorePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("not authenticated — run 'login' first")
		}
		return nil, fmt.Errorf("failed to read credentials file: %w", err)
	}

	var tokens StoredTokens
	if err := json.Unmarshal(data, &tokens); err != nil {
		return nil, fmt.Errorf("failed to parse credentials file: %w", err)
	}

	return &tokens, nil
}

// ClearTokens removes the credentials file from disk, effectively logging
// the user out.
func ClearTokens() error {
	path, err := TokenStorePath()
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove credentials file: %w", err)
	}

	return nil
}

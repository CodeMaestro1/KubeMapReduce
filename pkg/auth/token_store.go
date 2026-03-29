package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// StoredTokens holds the tokens persisted to disk by the CLI after login.
type StoredTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ServerURL    string `json:"server_url"`
}

// IsAccessExpired returns true when the access token has expired (or will
// expire within the next 30 seconds).
func (t *StoredTokens) IsAccessExpired() bool {
	return time.Now().Unix() >= t.ExpiresAt-30
}

// TokenStorePath returns the platform-appropriate path for the credentials
// file:
//
//	Windows: %APPDATA%\kubemapreduce\credentials.json
//	Other:   $XDG_CONFIG_HOME/kubemapreduce/credentials.json (defaults to ~/.config)
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

// SaveTokens persists tokens to the credentials file with restricted
// permissions (0600).
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

// LoadTokens reads the stored tokens from disk. Returns an error if the
// credentials file does not exist (i.e. the user has not logged in).
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

// ClearTokens removes the credentials file from disk.
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

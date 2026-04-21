package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultRetryAttempts  = 3
	defaultRetryBaseDelay = 200 * time.Millisecond
	maxAuthResponseBytes  = 1 << 20 // 1 MiB
)

// isRetryableStatus determines if an HTTP status code represents a transient
// failure.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// isRetryableError determines if a Go error represents a transient network
// issue.
func isRetryableError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		var nestedNetErr net.Error
		return errors.As(urlErr.Err, &nestedNetErr)
	}

	return false
}

// sleepWithContext pauses execution until either a timer expires or the
// context is cancelled.
func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ensureStatus returns an error if the response status is not the expected one.
//
// It reads the response body to provide a descriptive error message from the
// server.
func ensureStatus(resp *http.Response, expectedStatus int, operation string) error {
	if resp.StatusCode == expectedStatus {
		return nil
	}

	body, err := readBoundedResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to %s: status %d (failed to read response body: %v)", operation, resp.StatusCode, err)
	}

	return fmt.Errorf("failed to %s: status %d: %s", operation, resp.StatusCode, string(body))
}

// readBoundedResponseBody reads the entire response body while enforcing a
// safety limit.
//
// This prevents memory exhaustion attacks or bugs from oversized responses,
// capping memory usage at [maxAuthResponseBytes].
func readBoundedResponseBody(body io.Reader) ([]byte, error) {
	limited := io.LimitReader(body, maxAuthResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxAuthResponseBytes {
		return nil, fmt.Errorf("response body exceeds limit (%d bytes)", maxAuthResponseBytes)
	}

	return data, nil
}

// ensureCallStatus checks a pre-read response status.
func ensureCallStatus(status int, body []byte, expectedStatus int, operation string) error {
	if status == expectedStatus {
		return nil
	}

	return fmt.Errorf("failed to %s: status %d: %s", operation, status, string(body))
}

// extractUserIDFromLocation parses the User ID from a Keycloak Location header.
//
// Keycloak returns the newly created resource's URL (e.g., /users/{id}) in the
// Location header of a 201 Created response.
func extractUserIDFromLocation(location string) (string, error) {
	if location == "" {
		return "", fmt.Errorf("missing Location header for created user")
	}

	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid Location header")
	}

	userID := parts[len(parts)-1]
	if userID == "" {
		return "", fmt.Errorf("invalid user id in Location header")
	}

	return userID, nil
}

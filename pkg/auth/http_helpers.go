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
)

// isRetryableStatus returns true for HTTP status codes that indicate a
// transient server-side failure worth retrying.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// isRetryableError returns true when the error is a network-level issue that
// may resolve on retry.
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

// sleepWithContext pauses for the given duration, returning early if the
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

// ensureStatus reads the response body and returns a descriptive error when
// the status code does not match the expected value.
func ensureStatus(resp *http.Response, expectedStatus int, operation string) error {
	if resp.StatusCode == expectedStatus {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to %s: status %d (failed to read response body: %v)", operation, resp.StatusCode, err)
	}

	return fmt.Errorf("failed to %s: status %d: %s", operation, resp.StatusCode, string(body))
}

// ensureCallStatus checks a pre-read response body against an expected status.
func ensureCallStatus(status int, body []byte, expectedStatus int, operation string) error {
	if status == expectedStatus {
		return nil
	}

	return fmt.Errorf("failed to %s: status %d: %s", operation, status, string(body))
}

// extractUserIDFromLocation extracts the trailing path segment (user ID) from
// a Keycloak Location header value.
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

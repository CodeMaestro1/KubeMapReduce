package auth

import (
	"errors"
	"fmt"
)

// ServiceUnavailableError indicates that the authentication service (Keycloak)
// could not be reached or returned a transient failure.
//
// This error is used to distinguish between logical authentication failures
// (e.g., invalid credentials) and infrastructure failures that may warrant
// a retry or a 503 Service Unavailable response to the client.
type ServiceUnavailableError struct {
	// Operation describes what was being attempted (e.g. "get admin token").
	Operation string
	// Err is the underlying cause of the failure.
	Err       error
}

// Error returns a formatted error string.
func (e *ServiceUnavailableError) Error() string {
	if e == nil {
		return "authentication service unavailable"
	}

	if e.Operation == "" {
		if e.Err == nil {
			return "authentication service unavailable"
		}
		return fmt.Sprintf("authentication service unavailable: %v", e.Err)
	}

	if e.Err == nil {
		return fmt.Sprintf("authentication service unavailable while %s", e.Operation)
	}

	return fmt.Sprintf("authentication service unavailable while %s: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error.
func (e *ServiceUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsServiceUnavailable reports whether err (or any error in its chain) is a
// ServiceUnavailableError.
//
// Callers should use this instead of direct type assertion to handle
// wrapped errors properly.
func IsServiceUnavailable(err error) bool {
	var unavailableErr *ServiceUnavailableError
	return errors.As(err, &unavailableErr)
}

// NewServiceUnavailableError creates a new [ServiceUnavailableError] for the
// given operation and cause.
func NewServiceUnavailableError(operation string, err error) error {
	return &ServiceUnavailableError{Operation: operation, Err: err}
}

package auth

import (
	"errors"
	"fmt"
)

// ServiceUnavailableError indicates that the authentication service (Keycloak)
// could not be reached or returned a transient failure.
type ServiceUnavailableError struct {
	Operation string
	Err       error
}

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

func (e *ServiceUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsServiceUnavailable reports whether err (or any error in its chain) is a
// ServiceUnavailableError.
func IsServiceUnavailable(err error) bool {
	var unavailableErr *ServiceUnavailableError
	return errors.As(err, &unavailableErr)
}

// NewServiceUnavailableError creates a new ServiceUnavailableError.
func NewServiceUnavailableError(operation string, err error) error {
	return &ServiceUnavailableError{Operation: operation, Err: err}
}

package auth

import (
	"errors"
	"fmt"
	"testing"
)

func TestServiceUnavailableError_Error(t *testing.T) {
	testErr := errors.New("underlying error")

	tests := []struct {
		name      string
		operation string
		err       error
		isNil     bool
		want      string
	}{
		{
			name:  "nil receiver",
			isNil: true,
			want:  "authentication service unavailable",
		},
		{
			name:      "empty operation and nil error",
			operation: "",
			err:       nil,
			want:      "authentication service unavailable",
		},
		{
			name:      "empty operation with error",
			operation: "",
			err:       testErr,
			want:      fmt.Sprintf("authentication service unavailable: %v", testErr),
		},
		{
			name:      "with operation and nil error",
			operation: "get token",
			err:       nil,
			want:      "authentication service unavailable while get token",
		},
		{
			name:      "with operation and error",
			operation: "get token",
			err:       testErr,
			want:      fmt.Sprintf("authentication service unavailable while get token: %v", testErr),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err *ServiceUnavailableError
			if !tt.isNil {
				err = &ServiceUnavailableError{
					Operation: tt.operation,
					Err:       tt.err,
				}
			}

			if got := err.Error(); got != tt.want {
				t.Errorf("ServiceUnavailableError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServiceUnavailableError_Unwrap(t *testing.T) {
	testErr := errors.New("underlying error")

	tests := []struct {
		name  string
		isNil bool
		err   error
		want  error
	}{
		{
			name:  "nil receiver",
			isNil: true,
			want:  nil,
		},
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
		{
			name: "with error",
			err:  testErr,
			want: testErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err *ServiceUnavailableError
			if !tt.isNil {
				err = &ServiceUnavailableError{
					Err: tt.err,
				}
			}

			if got := err.Unwrap(); got != tt.want {
				t.Errorf("ServiceUnavailableError.Unwrap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsServiceUnavailable(t *testing.T) {
	testErr := &ServiceUnavailableError{Operation: "test"}
	wrappedErr := fmt.Errorf("wrapped: %w", testErr)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "different error",
			err:  errors.New("different error"),
			want: false,
		},
		{
			name: "ServiceUnavailableError",
			err:  testErr,
			want: true,
		},
		{
			name: "wrapped ServiceUnavailableError",
			err:  wrappedErr,
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsServiceUnavailable(tt.err); got != tt.want {
				t.Errorf("IsServiceUnavailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewServiceUnavailableError(t *testing.T) {
	operation := "test operation"
	testErr := errors.New("test error")

	err := NewServiceUnavailableError(operation, testErr)

	var unavailableErr *ServiceUnavailableError
	if !errors.As(err, &unavailableErr) {
		t.Fatalf("NewServiceUnavailableError() did not return a *ServiceUnavailableError")
	}

	if unavailableErr.Operation != operation {
		t.Errorf("NewServiceUnavailableError() Operation = %v, want %v", unavailableErr.Operation, operation)
	}

	if unavailableErr.Err != testErr {
		t.Errorf("NewServiceUnavailableError() Err = %v, want %v", unavailableErr.Err, testErr)
	}
}

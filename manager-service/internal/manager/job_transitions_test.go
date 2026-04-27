package manager

import (
	"errors"
	"testing"
)

func TestValidateJobTransition_Valid(t *testing.T) {
	cases := []struct {
		from, to string
	}{
		{"Pending", "Running"},
		{"Pending", "Cleaning"},
		{"Running", "Cleaning"},
		{"Cleaning", "Completed"},
		{"Cleaning", "Failed"},
		{"Cleaning", "Cancelled"},
	}
	for _, tc := range cases {
		t.Run(tc.from+"→"+tc.to, func(t *testing.T) {
			if err := ValidateJobTransition(tc.from, tc.to); err != nil {
				t.Errorf("expected nil, got %v", err)
			}
		})
	}
}

func TestValidateJobTransition_Invalid(t *testing.T) {
	cases := []struct {
		from, to string
	}{
		// Direct terminal bypass (must go through Cleaning)
		{"Running", "Completed"},
		{"Running", "Failed"},
		{"Running", "Cancelled"},
		{"Pending", "Completed"},
		{"Pending", "Failed"},
		{"Pending", "Cancelled"},
		// Terminal states have no outgoing edges
		{"Completed", "Running"},
		{"Completed", "Cleaning"},
		{"Failed", "Running"},
		{"Failed", "Cleaning"},
		{"Cancelled", "Running"},
		{"Cancelled", "Cleaning"},
		// Unknown states
		{"", "Running"},
		{"Running", ""},
		{"Unknown", "Running"},
	}
	for _, tc := range cases {
		t.Run(tc.from+"→"+tc.to, func(t *testing.T) {
			err := ValidateJobTransition(tc.from, tc.to)
			if err == nil {
				t.Errorf("expected ErrForbiddenTransition, got nil")
			}
			if !errors.Is(err, ErrForbiddenTransition) {
				t.Errorf("expected errors.Is(err, ErrForbiddenTransition), got %v", err)
			}
		})
	}
}

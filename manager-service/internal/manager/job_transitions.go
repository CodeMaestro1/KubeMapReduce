package manager

import "fmt"

// ErrForbiddenTransition is returned when a job state transition violates the
// allowed state machine. Callers can check for this with errors.Is.
var ErrForbiddenTransition = fmt.Errorf("forbidden job state transition")

// allowedJobTransitions defines valid directed edges in the job state machine.
//
//	Pending  → Running | Cleaning
//	Running  → Cleaning
//	Cleaning → Completed | Failed | Cancelled
//
// Terminal states (Completed, Failed, Cancelled) have no outgoing edges.
var allowedJobTransitions = map[string]map[string]bool{
	"Pending": {"Running": true, "Cleaning": true},
	"Running": {"Cleaning": true},
	"Cleaning": {
		"Completed": true,
		"Failed":    true,
		"Cancelled": true,
	},
}

// ValidateJobTransition returns nil if the from→to transition is permitted by
// the state machine, or a wrapped ErrForbiddenTransition otherwise.
func ValidateJobTransition(from, to string) error {
	if allowedJobTransitions[from][to] {
		return nil
	}
	return fmt.Errorf("%w: %s → %s", ErrForbiddenTransition, from, to)
}

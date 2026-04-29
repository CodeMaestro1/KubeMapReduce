package validation

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"kubemapreduce/manager-service/internal/models"
)

const (
	// MapInterface defines the expected function signature for Mapper tasks.
	//
	// This string is used to ensure the Worker node loads a compatible function.
	MapInterface = "map(key,value)->[]KeyValue"
	// ReduceInterface defines the expected function signature for Reducer tasks.
	//
	// This signature is also used for Combiner tasks, as they perform a local reduction.
	ReduceInterface = "reduce(key,values)->Value"
)

var allowedLanguages = map[string]struct{}{
	"python": {},
	"java":   {},
	"c":      {},
	"cpp":    {},
}

// ValidateJobSubmission checks if a job submission request meets system invariants.
//
// It validates that the input filename is safe (no path traversal), that the
// function specifications (Mapper, Reducer, optional Combiner) match the
// required interfaces, and that the resource requests (Reducer count) are sane.
// This early validation prevents malformed jobs from entering the scheduler and
// wasting cluster resources.
func ValidateJobSubmission(req models.JobSubmissionRequest) error {
	if req.Filename == "" {
		return NewBadRequestError("filename is required")
	}

	if strings.HasPrefix(req.Filename, "s3://") {
		u, err := url.Parse(req.Filename)
		if err != nil || u.Scheme != "s3" || u.Host == "" || strings.TrimSpace(u.Path) == "" {
			return NewBadRequestError("filename is invalid")
		}
	} else {
		clean := filepath.Clean(req.Filename)
		// Enforce that filename is a simple basename (no directories) and not a traversal token.
		// This prevents path traversal attacks where a user might try to read /etc/passwd or
		// write to sensitive system directories via the MapReduce input/output paths.
		if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.Base(clean) != clean {
			return NewBadRequestError("filename is invalid")
		}
	}

	if err := validateFunctionSpec("mapper", req.Mapper, MapInterface); err != nil {
		return err
	}

	if err := validateFunctionSpec("reducer", req.Reducer, ReduceInterface); err != nil {
		return err
	}

	if req.Combiner != nil {
		if err := validateFunctionSpec("combiner", *req.Combiner, ReduceInterface); err != nil {
			return err
		}
	}

	if req.Reducers < 1 {
		return NewBadRequestError("reducers must be a positive integer")
	}

	return nil
}

func validateFunctionSpec(functionName string, spec models.FunctionSpec, expectedInterface string) error {
	language := strings.ToLower(strings.TrimSpace(spec.Language))
	if _, ok := allowedLanguages[language]; !ok {
		return NewBadRequestError(functionName + ".language must be one of: python, java, c, cpp")
	}

	if strings.TrimSpace(spec.Artifact) == "" {
		return NewBadRequestError(functionName + ".artifact is required")
	}

	if strings.TrimSpace(spec.Entrypoint) == "" {
		return NewBadRequestError(functionName + ".entrypoint is required")
	}

	if strings.TrimSpace(spec.Interface) != expectedInterface {
		return NewBadRequestError(functionName + ".interface must be " + expectedInterface)
	}

	return nil
}

// ValidateCreateUserRequest ensures user creation payloads contain required fields and valid roles.
//
// It enforces that every user has a non-empty password and a role that the
// system recognizes (USER or ADMIN).
func ValidateCreateUserRequest(req models.CreateUserRequest) error {
	if req.Username == "" {
		return NewBadRequestError("username is required")
	}

	if req.Password == "" {
		return NewBadRequestError("password is required")
	}

	role := NormalizeRole(req.Role)
	if role != "USER" && role != "ADMIN" {
		return NewBadRequestError("role must be USER or ADMIN")
	}

	return nil
}

// NormalizeRole trims whitespace and converts the role string to uppercase.
//
// This allows the system to be case-insensitive during user input while
// maintaining a canonical representation in the database.
func NormalizeRole(role string) string {
	return strings.ToUpper(strings.TrimSpace(role))
}

// BadRequestError represents a client-side error caused by an invalid request payload.
//
// It is explicitly distinguishable from internal server errors, allowing the
// API layer to return a 400 Bad Request status instead of a generic 500 error.
type BadRequestError struct {
	message string
}

// NewBadRequestError creates a new instance of [BadRequestError].
func NewBadRequestError(message string) *BadRequestError {
	return &BadRequestError{message: message}
}

func (e *BadRequestError) Error() string {
	return e.message
}

// IsBadRequest returns true if the error is of type [*BadRequestError].
func IsBadRequest(err error) bool {
	_, ok := err.(*BadRequestError)
	return ok
}

// ErrorMessage returns the string representation of the error.
func ErrorMessage(err error) string {
	return fmt.Sprintf("%v", err)
}

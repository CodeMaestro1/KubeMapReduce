package validation

import (
	"fmt"
	"path/filepath"
	"strings"

	"kubemapreduce/manager-service/internal/models"
)

const (
	MapInterface    = "map(key,value)->[]KeyValue"
	ReduceInterface = "reduce(key,values)->Value"
)

var allowedLanguages = map[string]struct{}{
	"python": {},
	"java":   {},
	"c":      {},
	"cpp":    {},
}

func ValidateJobSubmission(req models.JobSubmissionRequest) error {
	if req.Filename == "" {
		return NewBadRequestError("filename is required")
	}

	clean := filepath.Clean(req.Filename)
	// Enforce that filename is a simple basename (no directories) and not a traversal token.
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.Base(clean) != clean {
		return NewBadRequestError("filename is invalid")
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

func NormalizeRole(role string) string {
	return strings.ToUpper(strings.TrimSpace(role))
}

type BadRequestError struct {
	message string
}

func NewBadRequestError(message string) *BadRequestError {
	return &BadRequestError{message: message}
}

func (e *BadRequestError) Error() string {
	return e.message
}

func IsBadRequest(err error) bool {
	_, ok := err.(*BadRequestError)
	return ok
}

func ErrorMessage(err error) string {
	return fmt.Sprintf("%v", err)
}

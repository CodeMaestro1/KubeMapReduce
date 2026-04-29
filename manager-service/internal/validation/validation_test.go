package validation

import (
	"errors"
	"testing"

	"kubemapreduce/manager-service/internal/models"
)

func TestValidateJobSubmission(t *testing.T) {
	tests := []struct {
		name    string
		req     models.JobSubmissionRequest
		wantErr string
	}{
		{
			name: "valid job submission",
			req:  validJobSubmissionRequest(),
		},
		{
			name:    "missing filename",
			req:     models.JobSubmissionRequest{},
			wantErr: "filename is required",
		},
		{
			name: "invalid filename traversal",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Filename = "../input.txt"
				return req
			}(),
			wantErr: "filename is invalid",
		},
		{
			name: "valid s3 filename",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Filename = "s3://inputs/job-123/input.txt"
				return req
			}(),
		},
		{
			name: "invalid mapper language",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Mapper.Language = "ruby"
				return req
			}(),
			wantErr: "mapper.language must be one of: python, java, c, cpp",
		},
		{
			name: "missing mapper artifact",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Mapper.Artifact = "   "
				return req
			}(),
			wantErr: "mapper.artifact is required",
		},
		{
			name: "missing reducer entrypoint",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Reducer.Entrypoint = ""
				return req
			}(),
			wantErr: "reducer.entrypoint is required",
		},
		{
			name: "invalid mapper interface",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Mapper.Interface = "wrong-interface"
				return req
			}(),
			wantErr: "mapper.interface must be " + MapInterface,
		},
		{
			name: "invalid reducer interface",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Reducer.Interface = "wrong-interface"
				return req
			}(),
			wantErr: "reducer.interface must be " + ReduceInterface,
		},
		{
			name: "valid job with combiner",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Combiner = &models.FunctionSpec{
					Language:   "python",
					Artifact:   "combiner.py",
					Entrypoint: "combine",
					Interface:  ReduceInterface,
				}
				return req
			}(),
		},
		{
			name: "invalid combiner language",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Combiner = &models.FunctionSpec{
					Language:   "ruby",
					Artifact:   "combiner.rb",
					Entrypoint: "combine",
					Interface:  ReduceInterface,
				}
				return req
			}(),
			wantErr: "combiner.language must be one of: python, java, c, cpp",
		},
		{
			name: "invalid combiner interface",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Combiner = &models.FunctionSpec{
					Language:   "python",
					Artifact:   "combiner.py",
					Entrypoint: "combine",
					Interface:  "wrong-interface",
				}
				return req
			}(),
			wantErr: "combiner.interface must be " + ReduceInterface,
		},
		{
			name: "negative reducers count",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Reducers = -1
				return req
			}(),
			wantErr: "reducers must be a positive integer",
		},
		{
			name: "zero reducers count",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Reducers = 0
				return req
			}(),
			wantErr: "reducers must be a positive integer",
		},
		{
			name: "valid job with reducers count",
			req: func() models.JobSubmissionRequest {
				req := validJobSubmissionRequest()
				req.Reducers = 5
				return req
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateJobSubmission(tc.req)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error %q, got nil", tc.wantErr)
			}
			if err.Error() != tc.wantErr {
				t.Fatalf("expected error %q, got %q", tc.wantErr, err.Error())
			}
			if !IsBadRequest(err) {
				t.Fatalf("expected bad request error type")
			}
		})
	}
}

func TestValidateCreateUserRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     models.CreateUserRequest
		wantErr string
	}{
		{
			name: "valid with lowercase role gets accepted",
			req: models.CreateUserRequest{
				Username: "alice",
				Password: "secret",
				Role:     "admin",
			},
		},
		{
			name: "valid with spaced role gets accepted",
			req: models.CreateUserRequest{
				Username: "alice",
				Password: "secret",
				Role:     " user ",
			},
		},
		{
			name: "missing username",
			req: models.CreateUserRequest{
				Password: "secret",
				Role:     "ADMIN",
			},
			wantErr: "username is required",
		},
		{
			name: "missing password",
			req: models.CreateUserRequest{
				Username: "alice",
				Role:     "ADMIN",
			},
			wantErr: "password is required",
		},
		{
			name: "invalid role",
			req: models.CreateUserRequest{
				Username: "alice",
				Password: "secret",
				Role:     "manager",
			},
			wantErr: "role must be USER or ADMIN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCreateUserRequest(tc.req)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error %q, got nil", tc.wantErr)
			}
			if err.Error() != tc.wantErr {
				t.Fatalf("expected error %q, got %q", tc.wantErr, err.Error())
			}
			if !IsBadRequest(err) {
				t.Fatalf("expected bad request error type")
			}
		})
	}
}

func TestNormalizeRole(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "uppercase admin", in: "admin", want: "ADMIN"},
		{name: "trim and uppercase user", in: " user ", want: "USER"},
		{name: "empty remains empty", in: "   ", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeRole(tc.in)
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestBadRequestHelpers(t *testing.T) {
	badReq := NewBadRequestError("bad input")
	if !IsBadRequest(badReq) {
		t.Fatal("expected NewBadRequestError to be recognized as bad request")
	}
	if ErrorMessage(badReq) != "bad input" {
		t.Fatalf("expected error message %q, got %q", "bad input", ErrorMessage(badReq))
	}

	plainErr := errors.New("plain error")
	if IsBadRequest(plainErr) {
		t.Fatal("expected plain error not to be recognized as bad request")
	}
}

func validJobSubmissionRequest() models.JobSubmissionRequest {
	return models.JobSubmissionRequest{
		Filename: "input.txt",
		Mapper: models.FunctionSpec{
			Language:   "python",
			Artifact:   "mapper.py",
			Entrypoint: "map",
			Interface:  MapInterface,
		},
		Reducer: models.FunctionSpec{
			Language:   "java",
			Artifact:   "Reducer.class",
			Entrypoint: "reduce",
			Interface:  ReduceInterface,
		},
		Reducers: 1,
	}
}

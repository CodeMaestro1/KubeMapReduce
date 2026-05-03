# Agent Instructions for this Repository

Welcome, AI Agent! When interacting with this repository, you must adhere to the following strict guidelines to ensure code quality, security, and stability.

## 1. Code Formatting and Static Analysis
Before finalizing any code modifications or submitting your work, you **must** run the following commands to format and check the code. CI will fail if these are not passing.

- Format code: `go fmt ./...`
- Vet code: `go vet ./...`
- Run linting (if applicable): `golangci-lint run`
- Tidy dependencies: `go mod tidy`

## 2. Unit Testing Requirement
**Mandatory**: If you write, modify, or refactor any code (especially Go code), you **must** write comprehensive unit tests for what you wrote.
- Test coverage must be maintained or increased.
- Tests should cover both happy paths and edge cases.
- Use `k8s.io/client-go/kubernetes/fake` for mocking Kubernetes clients and `github.com/DATA-DOG/go-sqlmock` for database mocks if needed.

## 3. Running Tests
You must ensure that all tests pass locally before completing your task.
- Run tests: `go test -v -race -cover ./...`

## 4. Vulnerability Checking
Security is critical. Always check for vulnerabilities in the dependencies and the codebase before submitting.
- Run vulnerability checks using govulncheck: `govulncheck ./...`
(Note: Ensure govulncheck is installed or available in your environment, `go install golang.org/x/vuln/cmd/govulncheck@latest` if needed).

## 5. Coding Standards & Conventions
- **Logging**: Use the standard library `log/slog` for all structured logging.
- **Architecture**: Respect the microservices architecture. Place code in the appropriate service directories (`auth-service`, `cli-service`, `manager-service`, `worker-service`).
- **Dependencies**: Keep dependencies organized. Run `go mod tidy` after adding or removing imports.

## Checklist Before Submission
Before you submit your changes, ensure you have:
- [ ] Run `go fmt ./...`
- [ ] Run `go vet ./...`
- [ ] Run `go mod tidy`
- [ ] Written unit tests for your new code/changes.
- [ ] Run `go test -v -race -cover ./...` and verified all tests pass.
- [ ] Run `govulncheck ./...` and resolved any new vulnerabilities introduced.

Failure to follow these instructions will result in CI failure and a rejected submission.

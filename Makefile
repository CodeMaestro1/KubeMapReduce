.PHONY: all build clean test fmt vet lint

# Build all services
all: build

# Build all Go services
build: cli manager api worker auth-setup

# Build CLI service
cli:
	@echo "Building CLI..."
	go build -o bin/cli ./cli-service/cmd/cli

# Build Manager service
manager:
	@echo "Building Manager..."
	go build -o bin/manager ./manager-service/cmd/manager

# Build API service
api:
	@echo "Building API..."
	go build -o bin/api ./manager-service/cmd/api

# Build Worker service
worker:
	@echo "Building Worker..."
	go build -o bin/worker ./worker-service/cmd/worker

# Build auth-setup service
auth-setup:
	@echo "Building auth-setup..."
	go build -o bin/auth-setup ./auth-service/cmd/setup

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -f bin/*

# Run tests
test:
	@echo "Running tests..."
	go test ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	go vet ./...

# Lint code (requires golangci-lint)
lint:
	@echo "Linting code..."
	golangci-lint run

# Help
help:
	@echo "Available targets:"
	@echo "  all     - Build all services (default)"
	@echo "  build   - Build all services"
	@echo "  cli     - Build CLI service"
	@echo "  manager - Build Manager service"
	@echo "  api     - Build API service"
	@echo "  worker  - Build Worker service"
	@echo "  auth-setup - Build auth-setup service"
	@echo "  clean   - Remove build artifacts"
	@echo "  test    - Run tests"
	@echo "  fmt     - Format code"
	@echo "  vet     - Vet code"
	@echo "  lint    - Lint code (requires golangci-lint)"
	@echo "  help    - Show this help"
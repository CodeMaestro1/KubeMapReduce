// Package main is the entry point for the UI Service API.
//
// # Overview
// The API server provides a RESTful interface for users and the KubeMapReduce CLI. 
// It handles user authentication (via Keycloak), job submission validation, 
// and job status querying. It acts as the orchestration layer between external 
// clients and the internal gRPC-based Manager Service.
//
// # Design Rationale
// This service is separated from the Manager to allow for independent scaling 
// of the user-facing API and the background scheduling logic. It uses 
// graceful shutdown to ensure that in-flight requests are completed before 
// the process terminates, preventing data loss during deployment.
//
// # Key Components
//   - JWT Validator: Ensures every request carries a valid token from Keycloak.
//   - Job Store: A PostgreSQL-backed repository for job metadata.
//   - HTTP Server: A production-ready server with tuned timeouts for stability.
//
// # Thread Safety
// The main loop is the only goroutine that manages the server lifecycle. 
// Handlers are safe for concurrent use as they rely on a thread-safe SQL 
// connection pool.
package main

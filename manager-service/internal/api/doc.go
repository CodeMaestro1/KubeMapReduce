// Package api implements the KubeMapReduce REST API and job management layer.
//
// # Overview
// The api package provides the primary interface for users and administrators
// to interact with the MapReduce cluster. It handles job submission, status
// tracking, result retrieval, and administrative tasks like user management
// and cluster node configuration.
//
// # Design Rationale
// This package is designed as a stateless HTTP layer that delegates persistence
// to a [JobStore] and authentication to Keycloak. By separating the API
// handlers from the storage implementation, the system can scale horizontally
// and switch between in-memory storage (for development/testing) and
// PostgreSQL-backed storage (for production) without changing the core
// business logic.
//
// # Key Types
// - [Handlers]: The main struct containing all HTTP handler methods.
// - [JobStore]: An interface for persisting and retrieving job metadata.
// - [JobRecord]: The internal representation of a job's status and configuration.
//
// # Thread Safety
// The [Handlers] struct is stateless and safe for concurrent use. The
// [JobStore] implementations are responsible for their own concurrency
// control (e.g., PostgreSQL transactions or mutex-protected maps).
//
// # Error Handling
// Handlers return standard HTTP error codes. Domain-specific errors like
// [ErrInvalidJobID] are used to provide more granular feedback when
// interacting with the [JobStore].
package api

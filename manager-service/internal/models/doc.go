// Package models defines the central data structures and schemas used by the KubeMapReduce platform.
//
// # Overview
// This package provides a single source of truth for the entities that populate the Distributed
// Data Store (DDS) and the request/response models used across the API and gRPC boundaries.
// It decouples the business logic of the Manager and Worker from the underlying database schema.
//
// # Design Rationale
// The models are designed to be "dumb" data containers. Logic is intentionally kept to a
// minimum, with [Job.IsTerminal] being a rare exception for convenience. By strictly mapping
// these Go structs to the DDS tables (via `db` tags), the system ensures that changes to the
// data model are explicit and easily traceable.
//
// # Key Types
//   - [Job]: The root entity representing a complete MapReduce job.
//   - [Task]: A unit of work belonging to a job (either Map or Reduce).
//   - [TaskAttempt]: Tracks an individual worker's execution of a task, owning the lease logic.
//   - [JobSubmissionRequest]: The primary payload for starting new work via the API.
//
// # Thread Safety
// These models are not thread-safe and should be treated as immutable once populated,
// unless explicitly protected by a mutex in the consuming package.
//
// # Error Handling
// This package does not define errors. Callers are responsible for validating these
// models using the [validation] package.
package models

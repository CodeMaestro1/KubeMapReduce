// Package grpc implements the gRPC server that handles communication with distributed workers.
//
// # Overview
// This package provides the [WorkerServer], which exposes the gRPC interface defined in
// `proto/mapreduce.proto`. It is the primary way workers interact with the Manager to
// claim tasks, report heartbeats, and commit results.
//
// # Design Rationale
// The gRPC server is designed as a thin translation layer. It handles network-level
// concerns (serialization, gRPC status codes, message size limits) and delegates all
// business logic and state transitions to the [manager.Scheduler]. This separation
// allows for easier testing and ensures that the core scheduling logic is not
// coupled to the transport protocol.
//
// # Key Types
//   - [WorkerServer]: The main gRPC service implementation.
//
// # Thread Safety
// The [WorkerServer] is safe for concurrent use as it relies on the [manager.Scheduler],
// which implements its own internal locking and transactional guarantees via the DDS.
//
// # Error Handling
// Errors from the scheduler are mapped to appropriate gRPC status codes (e.g.,
// [manager.ErrStaleAttempt] becomes `codes.PermissionDenied`). This ensures that
// workers receive actionable feedback when a request is rejected.
package grpc

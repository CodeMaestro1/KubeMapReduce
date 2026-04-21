// Package httputil contains reusable HTTP response utilities for the KubeMapReduce UI.
//
// # Overview
// This package standardizes how the UI Service communicates with REST clients. 
// It provides helper functions for JSON serialization and error reporting, 
// ensuring that all API responses follow a consistent format.
//
// # Design Rationale
// By abstracting the 'json.Marshal' and 'WriteHeader' sequence, the system 
// reduces boilerplate in HTTP handlers and guarantees that the 'Content-Type' 
// header is always set correctly. The addition of a trailing newline in 
// [WriteJSON] is a quality-of-life feature for CLI-based consumers of the API.
//
// # Key Types
//   - [WriteJSON]: The primary function for returning successful responses.
//   - [WriteError]: A helper for returning simple error strings.
//
// # Thread Safety
// Functions in this package are stateless and thread-safe.
//
// # Error Handling
// If [WriteJSON] fails to marshal the payload, it will automatically fall back 
// to [WriteError] with a 500 status code.
package httputil

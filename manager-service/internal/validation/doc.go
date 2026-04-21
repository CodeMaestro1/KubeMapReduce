// Package validation provides unified request validation and error classification.
//
// # Overview
// This package is the gatekeeper for the KubeMapReduce system. It ensures that 
// all external inputs—whether they are job submissions from the CLI or 
// user management requests from the UI—conform to the system's security and 
// operational invariants.
//
// # Design Rationale
// Centralizing validation logic ensures consistency across different API 
// endpoints. It specifically guards against common distributed system 
// vulnerabilities, such as path traversal in file references and 
// interface mismatch between Mappers and Workers. By returning a 
// specialized [BadRequestError], it allows the HTTP layer to provide 
// high-signal feedback to the user.
//
// # Key Types
//   - [BadRequestError]: A custom error type used to signal client-side input failures.
//   - [MapInterface]: The canonical string contract for Mapper functions.
//   - [ReduceInterface]: The canonical string contract for Reducer/Combiner functions.
//
// # Thread Safety
// Validation functions are stateless and safe for concurrent use.
//
// # Error Handling
// Callers should use [IsBadRequest] to determine if a returned error 
// warrants a 400 Bad Request response.
package validation

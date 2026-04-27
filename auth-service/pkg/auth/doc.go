// Package auth provides authentication and authorization primitives for the KubeMapReduce platform.
//
// # Overview
// This package centralizes all security logic, including JWT validation against Keycloak,
// role-based access control (RBAC) middleware, and management of user identities via
// the Keycloak Admin API. It ensures that every request within the system is authenticated
// and that workers are properly fenced using cryptographically signed tokens.
//
// # Design Rationale
// Security in KubeMapReduce is built on the principle of "Stateless Validation,
// Centralized Identity." By using JWTs, the Manager and other services can verify
// a caller's identity and roles without a per-request database lookup, provided
// the signature is valid according to the JWKS (JSON Web Key Set) published by Keycloak.
//
// # Key Types
// - [JWTValidator]: Middleware that validates Bearer tokens and populates request context.
// - [KeycloakAdminClient]: A thread-safe client for managing Keycloak users and roles.
// - [StoredTokens]: Represents the token persistence format used by the CLI.
//
// # Thread Safety
// [JWTValidator] and [KeycloakAdminClient] are safe for concurrent use by multiple goroutines.
// [KeycloakAdminClient] implements internal locking for administrative token refresh to
// prevent thundering herd problems when the admin session expires.
//
// # Error Handling
// The package uses a structured [ServiceUnavailableError] to signal transient failures
// in the identity provider (Keycloak), allowing callers to implement retry logic or
// return 503 Service Unavailable to clients.
package auth

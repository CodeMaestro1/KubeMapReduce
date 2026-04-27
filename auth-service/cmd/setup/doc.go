// Package main is the entry point for the Keycloak setup utility.
//
// # Overview
// The setup utility is a one-time bootstrap tool used to configure Keycloak
// for the KubeMapReduce platform. It creates the required realm, OIDC client,
// and roles, ensuring that the authentication service and manager can
// correctly validate tokens.
//
// # Design Rationale
// This is a separate CLI tool to ensure that environment-specific
// configuration (like realm names and client secrets) can be applied
// idempotently before the main services start. It avoids complex
// auto-bootstrap logic within the long-running services themselves.
//
// # Key Components
//   - Bootstrap Config: Handles flag and environment variable resolution.
//   - Keycloak Admin Client: Performs the REST calls to the Keycloak Master realm.
//
// # Thread Safety
// This tool is designed to be run as a single-instance job (e.g., a Kubernetes Job)
// and is not intended for concurrent use.
package main

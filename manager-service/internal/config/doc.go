// Package config handles the environmental configuration and bootstrap settings for the Manager Service.
//
// # Overview
// This package is responsible for mapping environment variables to a typed [Config] struct.
// It acts as the gatekeeper during service startup, ensuring that all required credentials
// and system tunables are present and valid.
//
// # Design Rationale
// The choice to use environment variables (sourced via [Load]) follows the 12-Factor App
// methodology. This allows the same binary to be deployed across different environments
// (dev, staging, prod) by simply changing the Kubernetes ConfigMap or Secret without
// modifying the code or including brittle configuration files in the container image.
//
// # Key Types
//   - [Config]: The central struct containing all typed configuration fields.
//
// # Thread Safety
// The [Config] struct is intended to be loaded once at startup and treated as read-only
// thereafter. It is safe for concurrent reads across the entire application.
//
// # Error Handling
// [Load] returns a descriptive error if environment variables that expect numeric values
// (like HEARTBEAT_INTERVAL_SEC) contain non-numeric data, preventing the service from
// starting in an undefined state.
package config

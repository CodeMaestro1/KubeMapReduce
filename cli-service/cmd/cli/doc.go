// Package main implements the KubeMapReduce Command Line Interface (CLI).
//
// # Overview
// The KubeMapReduce CLI is the primary user interface for interacting with the
// distributed MapReduce platform. It provides commands for job submission,
// monitoring, and management, as well as administrative tasks like user
// creation and cluster configuration.
//
// # Design Rationale
// The CLI is designed as a stateless client that communicates with the API
// service via REST. It handles authentication by exchanging user credentials
// for JWT tokens from Keycloak, which are then persisted locally.
//
// To support automated environments (CI/CD, scripts, and pipes), the CLI
// robustly handles non-TTY terminals. Interactive prompts are suppressed and
// redirected to stderr, while sensitive inputs like passwords can be provided
// via stdin pipes or dedicated flags. This ensures the CLI is equally
// effective as a human-facing tool and a machine-facing integration point.
//
// # Key Commands
// [cmdLogin] - Authenticates the user and stores JWT tokens.
// [cmdJobsSubmit] - Submits new MapReduce jobs to the cluster.
// [cmdJobsStatus] - Monitors the progress of submitted jobs.
// [cmdAdminCreateUser] - Administrative command for managing system users.
//
// # Execution Environment
// The CLI is a single-threaded application. It uses a synchronized shared
// reader for stdin to prevent buffering conflicts when reading multiple
// inputs from a pipe in non-TTY environments.
//
// # Error Handling
// The CLI uses a fail-fast approach, logging fatal errors and exiting with a
// non-zero status code when an operation cannot be completed. It provides
// detailed error messages from the API server to aid in troubleshooting.
package main

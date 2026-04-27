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
// for JWT tokens from Keycloak, which are then persisted locally. This approach
// minimizes the amount of logic required in the CLI and ensures that all
// security and business rules are enforced consistently by the central API.
//
// # Key Commands
// [cmdLogin] - Authenticates the user and stores JWT tokens.
// [cmdJobsSubmit] - Submits new MapReduce jobs to the cluster.
// [cmdJobsStatus] - Monitors the progress of submitted jobs.
// [cmdAdminCreateUser] - Administrative command for managing system users.
//
// # Thread Safety
// The CLI is a single-threaded interactive application and is not intended for
// concurrent use within the same process.
//
// # Error Handling
// The CLI uses a fail-fast approach, logging fatal errors and exiting with a
// non-zero status code when an operation cannot be completed. It provides
// detailed error messages from the API server to aid in troubleshooting.
package main

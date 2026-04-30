package main

import (
	"fmt"
	"log"
	"os"
)

// main is the entry point for the KubeMapReduce CLI.
//
// It handles command-line argument parsing and dispatches execution to specific
// command handlers. This centralized dispatch pattern ensures that the CLI
// remains a thin wrapper over the underlying API services while providing a
// consistent user experience for both standard users and administrators.
func main() {
	log.SetFlags(0)
	log.SetPrefix("kubemapreduce: ")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "login":
		cmdLogin(os.Args[2:])
	case "logout":
		cmdLogout()
	case "health":
		cmdHealth()
	case "jobs":
		if len(os.Args) < 3 {
			log.Fatal("usage: kubemapreduce jobs <submit|list|status|download|cancel|delete> [flags]")
		}
		switch os.Args[2] {
		case "submit":
			cmdJobsSubmit(os.Args[3:])
		case "list":
			cmdJobsList()
		case "status":
			cmdJobsStatus(os.Args[3:])
		case "download":
			cmdJobsDownload(os.Args[3:])
		case "cancel", "delete":
			cmdJobsCancel(os.Args[3:])
		default:
			log.Fatalf("unknown jobs subcommand: %s", os.Args[2])
		}
	case "admin":
		if len(os.Args) < 3 {
			log.Fatal("usage: kubemapreduce admin <create-user|delete-user|worker-config|configure-nodes> [flags]")
		}
		switch os.Args[2] {
		case "create-user":
			cmdAdminCreateUser(os.Args[3:])
		case "delete-user":
			cmdAdminDeleteUser(os.Args[3:])
		case "worker-config":
			cmdAdminWorkerConfig(os.Args[3:])
		case "configure-nodes":
			cmdAdminConfigureNodes(os.Args[3:])
		default:
			log.Fatalf("unknown admin subcommand: %s", os.Args[2])
		}
	case "whoami":
		cmdWhoAmI()
	case "token":
		if len(os.Args) >= 3 && os.Args[2] == "inspect" {
			cmdTokenInspect()
		} else {
			log.Fatal("usage: kubemapreduce token inspect")
		}
	case "help", "--help", "-h":
		printUsage()
	default:
		log.Fatalf("unknown command: %s\nRun 'kubemapreduce help' for usage.", os.Args[1])
	}
}

// printUsage outputs the help text for the CLI to standard output.
//
// This is used as the default fallback when the user provides no arguments or
// an unknown command, ensuring the CLI is self-documenting for new users.
func printUsage() {
	fmt.Print(`KubeMapReduce CLI

Usage:
  kubemapreduce <command> [flags]

Commands:
  login                  Authenticate with Keycloak and store tokens
  logout                 Clear stored authentication tokens
  health                 Check API server health
  jobs submit            Submit a MapReduce job (use --mapper, --reducer, --input; see jobs submit --help)
  jobs list              List all submitted jobs
  jobs status --id <id>           Show the status of a specific job
  jobs download --id <id>         Download completed job results (--output defaults to ./results/)
  jobs cancel --id <id>           Cancel a running or submitted job (or use: jobs delete --id <id>)
  whoami                 Show the currently logged-in user
  admin create-user      Create a user in Keycloak (ADMIN)
  admin delete-user      Delete a user from Keycloak (ADMIN)
  admin worker-config    Update worker configuration (ADMIN)
  admin configure-nodes  Set per-node resource limits (ADMIN): --max-pods, --cpu-limit, --memory-limit
  token inspect          Show raw JWT claims for the stored access token
  help                   Show this help message

Environment Variables:
  API_URL                API server URL          (default: http://localhost:8081)
  KEYCLOAK_BASE_URL      Keycloak base URL       (default: http://localhost:8080)
  KEYCLOAK_REALM         Keycloak realm          (default: mapreduce)
  KEYCLOAK_AUDIENCE      Keycloak client ID      (default: mapreduce-api)
`)
}

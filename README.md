# KubeMapReduce

Go API with Keycloak authentication, role-based authorization, and a dedicated CLI for authentication, job submission, and user management.

## Prerequisites

- Go 1.25+
- Docker + Docker Compose

## Local Development

```bash
cd infra/docker
docker compose up -d
```

| Service    | URL / Port                          |
|------------|-------------------------------------|
| Keycloak   | http://localhost:8080               |
| API        | http://localhost:8081               |
| PostgreSQL | localhost:5432 (user/db: mapreduce) |
| MinIO S3   | localhost:9000                      |
| MinIO UI   | http://localhost:9001               |

Run DB migrations after first start:

```bash
psql postgres://mapreduce:mapreduce@localhost:5432/mapreduce < migrations/0001_initial_schema.sql
```

## Quick Start

1. Start all services:

   ```bash
   cd infra/docker
   docker compose up -d
   ```

2. Run setup (bootstraps realm + creates first admin user):

   ```bash
   go run ./auth-service/cmd/setup \
     --admin-username admin \
     --admin-password admin \
     --username platform-admin \
     --email platform-admin@example.com \
     --prompt-password \
     --role ADMIN
   ```

   Omit `--username` to only bootstrap the realm without creating a user.

3. Run Manager API:

   ```bash
   go run ./manager-service/cmd/api
   ```

4. Use the CLI to authenticate and interact with the API:

   ```bash
   go run ./cli-service/cmd/cli login
   go run ./cli-service/cmd/cli jobs submit job.json
   ```

## Repository Layout

This repository now follows a polyrepo-ready microservice layout:

```text
cli-service/      # UI service (CLI entrypoint + CLI logic)
manager-service/  # manager API, scheduler, task/state orchestration
auth-service/     # authentication/bootstrap logic and auth libraries
k8s/              # Kubernetes manifests
infra/            # infra/deploy assets (docker-compose, env files, etc.)
```

Service code is isolated under the service folders so each service can be extracted into its own repository later.

## URLs

- API: `http://localhost:8081`
- Health check: `http://localhost:8081/health`
- Keycloak Admin Console: `http://localhost:8080`

## Authentication and Roles

The CLI authenticates with Keycloak using **username/password** (Resource Owner
Password Credentials grant) and stores a dual-token pair (short-lived access
token + longer-lived refresh token) in a local credentials file. Tokens are
refreshed automatically when they expire.

- API role rules:
  - `/jobs`: `USER` or `ADMIN`
  - `/admin/*`: `ADMIN` only

### Architecture

All admin user management flows through the API server:

```
Admin -> CLI -> API Server (POST /admin/users, DELETE /admin/users/{username}) -> Keycloak
```

The API server holds the Keycloak admin credentials and proxies create/delete
requests. The CLI never talks directly to Keycloak for user management — it
only needs a valid JWT with the `ADMIN` role.

## CLI Usage

```
kubemapreduce <command> [flags]

Commands:
  login                  Authenticate with Keycloak and store tokens
  logout                 Clear stored authentication tokens
  whoami                 Show the currently logged-in user
  health                 Check API server health
  jobs submit <file>     Submit a MapReduce job specification (use "-" for stdin)
  admin create-user      Create a user in Keycloak (ADMIN)
  admin delete-user      Delete a user from Keycloak (ADMIN)
  admin worker-config    Update worker configuration (ADMIN)
  token inspect          Show raw JWT claims for the stored access token
  help                   Show this help message
```

### Login

```bash
go run ./cli-service/cmd/cli login
# Username: alice
# Password: ********
# Login successful!
# Credentials saved to C:\Users\alice\AppData\Roaming\kubemapreduce\credentials.json
```

You can also pass `--username alice` to skip the username prompt.

### Submit a Job

> **Note:** Job submission currently validates and stores the job _specification_
> (metadata) only. No input data or code artifacts are transferred to the server.
> File transfer will be added in a future release.

```bash
go run ./cli-service/cmd/cli jobs submit job.json
```

### Admin Commands

Admin commands require the `ADMIN` role. User management routes through the
API server, which proxies to Keycloak:

```bash
# Create a user (prompts for the new user's password)
go run ./cli-service/cmd/cli admin create-user --username bob --email bob@example.com --prompt-password --role USER

# Delete a user
go run ./cli-service/cmd/cli admin delete-user --username bob

# Update worker configuration
go run ./cli-service/cmd/cli admin worker-config --replicas 4 --max-jobs 8
```

All three commands check the local token for the `ADMIN` role before making
the request. The API server re-validates the JWT and enforces the role
server-side as well.

### Who Am I

```bash
go run ./cli-service/cmd/cli whoami
# Username: platform-admin
# Email:    platform-admin@example.com
# Subject:  a1b2c3d4-...
# Roles:    ADMIN, default-roles-mapreduce
```

### Token Inspect

Dump the full raw JWT claims (useful for debugging):

```bash
go run ./cli-service/cmd/cli token inspect
```

## Initial Setup

The `setup` command bootstraps the Keycloak realm (client, roles, audience mapper)
and optionally creates the first user — all in one step:

```bash
go run ./auth-service/cmd/setup \
  --admin-username admin \
  --admin-password admin \
  --username platform-admin \
  --email platform-admin@example.com \
  --prompt-password \
  --role ADMIN
```

Notes:

- Use `--prompt-password` (recommended) so the new user password is hidden.
- Use `--role USER` to create a normal user instead of an admin.
- Omit `--username` entirely to only bootstrap the realm without creating any users.
- After setup, use `kubemapreduce admin create-user` via the CLI for additional users.

## Configuration

Environment variables used by the API server (defaults shown):

- `KEYCLOAK_BASE_URL` (`http://localhost:8080`)
- `KEYCLOAK_REALM` (`mapreduce`)
- `KEYCLOAK_JWKS_URL` (`http://localhost:8080/realms/mapreduce/protocol/openid-connect/certs`)
- `KEYCLOAK_ISSUER` (`http://localhost:8080/realms/mapreduce`)
- `KEYCLOAK_AUDIENCE` (`mapreduce-api`)
- `KEYCLOAK_ADMIN_USERNAME` (**required**) — Keycloak admin user for proxied user management
- `KEYCLOAK_ADMIN_PASSWORD` (**required**) — Keycloak admin password
- `SERVER_ADDR` (`:8081`)

The API fails fast during startup if either `KEYCLOAK_ADMIN_USERNAME` or
`KEYCLOAK_ADMIN_PASSWORD` is missing or blank.

Environment variables used by the CLI:

- `API_URL` (`http://localhost:8081`)
- `KEYCLOAK_BASE_URL` (`http://localhost:8080`)
- `KEYCLOAK_REALM` (`mapreduce`)
- `KEYCLOAK_AUDIENCE` (`mapreduce-api`)

## Token Storage

After login, credentials are stored in a platform-appropriate config directory:

- **Windows**: `%APPDATA%\kubemapreduce\credentials.json`
- **Linux/macOS**: `$XDG_CONFIG_HOME/kubemapreduce/credentials.json` (defaults to `~/.config`)

The file is created with `0600` permissions. The stored structure:

```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "expires_at": 1682349600,
  "server_url": "http://localhost:8081"
}
```

When the access token expires, the CLI silently refreshes it using the refresh
token. If the refresh token itself has expired, the user is prompted to log in
again.

For request base URL resolution, the CLI uses deterministic precedence:

1. `server_url` from stored credentials, when present.
2. `API_URL` environment variable (or `http://localhost:8081` if unset).

Legacy credentials that do not contain `server_url` are supported. The CLI
falls back to `API_URL` and best-effort persists the resolved value back to
the credentials file for future runs.

## API Endpoints

| Method   | Path                      | Auth              | Description                                  |
| -------- | ------------------------- | ----------------- | -------------------------------------------- |
| `GET`    | `/`                       | None              | API info (JSON)                              |
| `GET`    | `/health`                 | None              | Liveness check                               |
| `POST`   | `/jobs`                   | `USER` or `ADMIN` | Submit a MapReduce job spec (metadata only)  |
| `PUT`    | `/admin/workers/config`   | `ADMIN`           | Update worker configuration                  |
| `POST`   | `/admin/users`            | `ADMIN`           | Create a user in Keycloak                    |
| `DELETE` | `/admin/users/{username}` | `ADMIN`           | Delete a user from Keycloak (204 No Content) |

The `DELETE /admin/users/{username}` endpoint returns **204 No Content** with no
response body. It does not require a request body either.

Some HTTP clients and proxies do not fully support bodies on `DELETE` requests, so the username is supplied in the path instead.

All protected endpoints require a Bearer token from Keycloak.

## E2E Failure Validation (INF-419)

KubeMapReduce includes a dedicated end-to-end failure-injection suite to validate the system's resilience against infrastructure failures, as required for the INF-419 Principles of Distributed Systems deliverable.

### Prerequisites

- A running Kubernetes cluster (Kind, Minikube, or EKS/GKE).
- `kubectl` configured to point to the cluster.
- KubeMapReduce deployed to the cluster with the following resources:
  - Manager: a StatefulSet named `manager`
  - Worker Jobs: labeled with `app=kubemapreduce-worker`
- Active CLI authentication (`kubemapreduce login`).

### Running the Validation Suite

The suite automates three high-impact failure scenarios:
1. **Worker Pod Kill**: Deletes a worker pod mid-execution to verify task reassignment.
2. **Manager Restart**: Restarts the manager StatefulSet to verify recovery of active attempts from the DDS.
3. **Zombie Fencing**: Pauses a worker (SIGSTOP) until its lease expires, then resumes it (SIGCONT) to verify that its subsequent commit attempt is rejected by the manager.

Execute the suite from the repository root:

```bash
./scripts/e2e_failure_injection.sh
```

### Reports and Artifacts

Each run generates a timestamped report directory in `./reports/`. This directory contains:
- `suite.log`: Detailed execution trace with timestamps.
- `zombie_worker.log`: Captured logs from the fenced worker showing rejection errors (e.g., `PermissionDenied`).

These artifacts provide the necessary evidence for the final validation report and presentation.

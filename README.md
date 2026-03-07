# KubeMapReduce

Go API with Keycloak authentication, role-based authorization, and a dedicated CLI for authentication and job submission.

## Prerequisites

- Go 1.25+
- Docker + Docker Compose

## Quick Start

1. Start Keycloak:

   ```bash
   cd docker
   docker-compose up -d
   ```

2. Run setup (bootstraps realm + creates first admin user):

   ```bash
   go run ./cmd/setup \
     --admin-username admin \
     --admin-password admin \
     --username platform-admin \
     --email platform-admin@example.com \
     --prompt-password \
     --role ADMIN
   ```

   Omit `--username` to only bootstrap the realm without creating a user.

3. Run API (project root):

   ```bash
   go run ./cmd/api
   ```

4. Use the CLI to authenticate and interact with the API:

   ```bash
   go run ./cmd/cli login
   go run ./cmd/cli jobs submit job.json
   ```

## URLs

- API: `http://localhost:8081`
- Health check: `http://localhost:8081/health`
- Keycloak Admin Console: `http://localhost:8080`

## Authentication and Roles

The CLI authenticates with Keycloak using **email/password** and stores a
dual-token pair (short-lived access token + longer-lived refresh token) in a
local credentials file. Tokens are refreshed automatically when they expire.

- Self-registration creates regular realm users.
- API role rules:
  - `/jobs`: `USER` or `ADMIN`
  - `/admin/*`: `ADMIN` only

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
go run ./cmd/cli login
# Username: alice
# Password: ********
# Login successful!
# Credentials saved to C:\Users\alice\AppData\Roaming\kubemapreduce\credentials.json
```

You can also pass `--username alice` to skip the username prompt.

### Submit a Job

```bash
go run ./cmd/cli jobs submit job.json
```

### Admin Commands

User management commands talk directly to Keycloak (no API server needed).
The `--admin-username` / `--admin-password` flags default to `admin`/`admin`.

```bash
# Create a user (prompts for the new user's password)
go run ./cmd/cli admin create-user --username bob --email bob@example.com --prompt-password --role USER

# Delete a user
go run ./cmd/cli admin delete-user --username bob

# Worker config goes through the API (requires ADMIN JWT)
go run ./cmd/cli admin worker-config --replicas 4 --max-jobs 8
```

### Who Am I

```bash
go run ./cmd/cli whoami
# Username: platform-admin
# Email:    platform-admin@example.com
# Subject:  a1b2c3d4-...
# Roles:    ADMIN, default-roles-mapreduce
```

### Token Inspect

Dump the full raw JWT claims (useful for debugging):

```bash
go run ./cmd/cli token inspect
```

## Initial Setup

The `setup` command bootstraps the Keycloak realm (client, roles, audience mapper)
and optionally creates the first user — all in one step:

```bash
go run ./cmd/setup \
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

Environment variables used by API (defaults shown):

- `KEYCLOAK_BASE_URL` (`http://localhost:8080`)
- `KEYCLOAK_REALM` (`mapreduce`)
- `KEYCLOAK_JWKS_URL` (`http://localhost:8080/realms/mapreduce/protocol/openid-connect/certs`)
- `KEYCLOAK_ISSUER` (`http://localhost:8080/realms/mapreduce`)
- `KEYCLOAK_AUDIENCE` (`mapreduce-api`)

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

## API Endpoints

- `GET /` - API info (JSON)
- `GET /health` - liveness
- `POST /jobs` - submit job spec (`USER` or `ADMIN`)
- `PUT /admin/workers/config` - update worker config (`ADMIN`)

All protected endpoints require a Bearer token from Keycloak.

## Common Issues

- JWKS `404` on startup: realm/client missing or wrong realm settings.
- `listen tcp :8081 bind`: another process is already using port 8081.
- `session expired, please login again`: refresh token expired — run `login` again.

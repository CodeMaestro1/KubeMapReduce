# KubeMapReduce

Go API with Keycloak authentication, role-based authorization, and a built-in browser Authentication Console.

## Prerequisites
- Go 1.25+
- Docker + Docker Compose

## Quick Start
1. Start Keycloak:
   ```bash
   cd docker
   docker-compose up -d
   ```
2. Bootstrap Keycloak auth configuration (realm/client/roles/redirects; no users created): (optional)
  ```bash
  go run ./cmd/auth-bootstrap \
    --admin-username admin \
    --admin-password admin
  ```
3. Run API (project root):
   ```bash
   go run ./cmd/api
   ```

## URLs
- API: `http://localhost:8081`
- Health check: `http://localhost:8081/health`
- Authentication Console UI: `http://localhost:8081/`
- Keycloak Admin Console: `http://localhost:8080` 

## Authentication and Roles
- `Sign in` and `Create account` in the Authentication Console use Keycloak.
- Self-registration creates regular realm users.
- API role rules:
  - `/jobs`: `USER` or `ADMIN`
  - `/admin/*`: `ADMIN` only

## Create Administrators (Recommended Path)
Use the dedicated CLI:

```bash
go run ./cmd/auth-admin-create \
  --admin-username admin \
  --admin-password admin \
  --username platform-admin \
  --email platform-admin@example.com \
  --prompt-password \
  --role ADMIN
```

Notes:
- Use `--prompt-password` (recommended) so the new user password is hidden.
- Use `--role USER` to create a normal user.

## Configuration
Environment variables used by API (defaults shown):
- `KEYCLOAK_BASE_URL` (`http://localhost:8080`)
- `KEYCLOAK_REALM` (`mapreduce`)
- `KEYCLOAK_JWKS_URL` (`http://localhost:8080/realms/mapreduce/protocol/openid-connect/certs`)
- `KEYCLOAK_ISSUER` (`http://localhost:8080/realms/mapreduce`)
- `KEYCLOAK_AUDIENCE` (`mapreduce-api`)
- `KEYCLOAK_ADMIN_USERNAME` (required for admin user management endpoints)
- `KEYCLOAK_ADMIN_PASSWORD` (required for admin user management endpoints)

## API Endpoints
- `GET /health` - liveness
- `POST /jobs` - submit job spec (`USER` or `ADMIN`)
- `POST /admin/users` - create user (`ADMIN`)
- `DELETE /admin/users/{username}` - delete user (`ADMIN`)
- `PUT /admin/workers/config` - update worker config (`ADMIN`)

All protected endpoints require a Bearer token from Keycloak.

## Common Issues
- `invalid parameter: redirect_uri`: client redirect/web origin not set for `http://localhost:8081/*`.
- JWKS `404` on startup: realm/client missing or wrong realm settings.
- `listen tcp :8081 bind`: another process is already using port 8081.
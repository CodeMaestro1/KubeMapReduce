# `auth` Package

The `auth` package provides authentication and authorization primitives for the KubeMapReduce platform.

## What It Does

- Validates Bearer JWTs against Keycloak JWKS data.
- Provides middleware for HTTP handlers.
- Exposes a thread-safe Keycloak admin client.
- Persists CLI tokens locally for user sessions.

## Design Goals

The package is built around two invariants:

1. Requests are validated statelessly through signed tokens.
2. Administrative and user-facing identity flows share the same security model.

## Main Types

- `JWTValidator`
- `KeycloakAdminClient`
- `StoredTokens`

## Source

See the package source in [auth-service/pkg/auth](../../../auth-service/pkg/auth) for the implementation details and exported API.

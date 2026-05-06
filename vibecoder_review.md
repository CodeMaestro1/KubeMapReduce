# Vibecoder Security Review: KubeMapReduce

**Date:** 2024-05-18

## Summary

Found 1 medium-priority issue in this Go application. The codebase has already been substantially hardened against common "vibecoder" vulnerabilities (e.g., uses secure execution sandboxes, no hardcoded secrets, uses standard JWT libraries).

## Findings

### [MEDIUM] MinIO pre-signed URLs Path Traversal via Weak Regex

**Location:** `manager-service/internal/api/handlers.go:56`

**Issue:** The regular expression used to validate filenames for MinIO pre-signed URLs (`safeFilenamePattern`) is intended to block directory traversal attacks. However, the regex `^[a-zA-Z0-9][a-zA-Z0-9._\-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$` permits consecutive dots (e.g., `a..b`), which bypasses the intended traversal protection.

```go
var safeFilenamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._\-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$`)
```

**Impact:** An attacker could craft filenames containing `..` sequences to bypass the upload validation, potentially writing to arbitrary keys or traversing directories in the input bucket.

**Evidence:**
- The comment explicitly states: `Rejects patterns like ".", "..", "...", "a..b", etc.`
- The pattern `[a-zA-Z0-9._\-]*` matches any sequence of dots, underscores, or hyphens, allowing `a..b` to pass validation.

**Remediation:** Update the regex to strictly disallow consecutive dots.
```go
var safeFilenamePattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9_\-]|(?:\.[a-zA-Z0-9_\-]))*[a-zA-Z0-9]?$|^[a-zA-Z0-9]$`)
```

## Quick Wins

1. Update `safeFilenamePattern` in `handlers.go` to strictly prevent consecutive dots.
2. The remaining parts of the application (JWT parsing, sandboxing, authentication, SQL queries) are following secure patterns.

## Context

**Stack:** Go, Kubernetes, MinIO, PostgreSQL, Keycloak
**Environment:** Production-grade distributed map-reduce.
**Auth pattern:** JWT bearer tokens via Keycloak.

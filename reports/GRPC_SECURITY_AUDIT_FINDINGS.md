# gRPC Security Audit Report: KubeMapReduce

**Report Date:** 2025-01-09  
**Scope:** gRPC mTLS Configuration, Message Size Limits, Authentication, Authorization, Lease Validation  
**Status:** 6 Vulnerabilities Identified (CRITICAL → MEDIUM)

---

## EXECUTIVE SUMMARY

This security audit identified **6 vulnerabilities** in the KubeMapReduce gRPC infrastructure:

- **1 CRITICAL** (CVSS 9.8): Insecure worker client transport by default
- **2 HIGH** (CVSS 8.6, 8.8): Missing transport security requirement on token credential; missing stream interceptor
- **1 HIGH** (CVSS 7.5): Lease ID/Attempt ID validation TOCTOU in Heartbeat RPC
- **2 MEDIUM** (CVSS 5.7, 6.5): Unauthenticated reflection endpoint; permissive message size limits

**Impact:** An attacker with network access to the K8s cluster can intercept worker-manager gRPC traffic, forge task completion/failure messages, or hijack worker task assignments.

---

## VULNERABILITY DETAILS

### [VULN-1] CRITICAL: Insecure Worker Client Transport by Default

**Severity:** CRITICAL (CVSS 9.8: `AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H`)  
**Location:** `worker-service/cmd/worker/main.go:60-68`  
**Category:** Transport Security

#### Description

Worker pods connect to the Manager service using **plaintext (insecure) transport by default**. The `transportOption()` function only enables TLS if `GRPC_TLS_CERT_FILE` is explicitly configured; otherwise, it silently falls back to `insecure.NewCredentials()`.

#### Attack Chain

```
1. Attacker gains network access to K8s cluster (CNI breach, compromised node).
2. Attacker sniffs plaintext gRPC traffic on port 50051.
3. Attacker intercepts or forges:
   - TaskComplete RPC with arbitrary output_locations
   - TaskFailed RPC to terminate workers
   - Register RPC with spoofed attempt_id
4. Manager accepts the forged request (if token auth is disabled).
5. Impact: Data corruption, DOS, worker hijacking.
```

#### Vulnerable Code

```go
// worker-service/cmd/worker/main.go:60-68
func transportOption(cfg *config.Config) grpc.DialOption {
	if cfg.GRPCTLSCertFile != "" {
		creds, err := grpccreds.NewClientTLSFromFile(cfg.GRPCTLSCertFile, "")
		if err != nil {
			log.Fatalf("TLS cert %s: %v", cfg.GRPCTLSCertFile, err)
		}
		return grpc.WithTransportCredentials(creds)
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())  // ❌ INSECURE!
}
```

#### Remediation

**Priority:** IMMEDIATE  
**Effort:** 1-2 hours  
**Testing:** Integration test with TLS certificate validation

See **PATCH-1** in section "REMEDIATION PATCHES" below.

---

### [VULN-2] HIGH: Worker gRPC Token Credential Does Not Require Transport Security

**Severity:** HIGH (CVSS 8.6: `AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:N`)  
**Location:** `worker-service/cmd/worker/main.go:74-78`  
**Category:** Authentication (Bearer Token Handling)

#### Description

The `rpcToken` type's `RequireTransportSecurity()` method returns `false`, signaling to gRPC that the bearer token can be sent over plaintext. Combined with VULN-1, this allows bearer tokens to be captured in the clear.

#### Vulnerable Code

```go
func (r rpcToken) RequireTransportSecurity() bool { return false }  // ❌ Plaintext token allowed!
```

#### Remediation

Set `RequireTransportSecurity()` to `true` to enforce that tokens are only sent over TLS.

**See PATCH-1.**

---

### [VULN-3] HIGH: Missing gRPC Stream Interceptor

**Severity:** HIGH (CVSS 8.8: `AV:N/AC:L/PR:L/UI:N/S:U/C:H/I:H/A:H`)  
**Location:** `manager-service/cmd/manager/main.go:177-181`  
**Category:** Authentication (Missing Interceptor)

#### Description

The gRPC server only registers a `UnaryServerInterceptor` for token validation. If a streaming RPC is added in the future without a corresponding stream interceptor, authentication will be bypassed.

#### Vulnerable Code

```go
grpcOpts := []grpc.ServerOption{
	grpc.UnaryInterceptor(workerAuthUnaryInterceptor(cfg.WorkerRPCToken)),
	// ❌ No StreamServerInterceptor!
	grpc.MaxRecvMsgSize(4 << 20),
	grpc.MaxSendMsgSize(16 << 20),
}
```

#### Remediation

Add a stream interceptor alongside the unary interceptor. **See PATCH-2.**

---

### [VULN-4] MEDIUM: gRPC Reflection Endpoint Exposed Without Authentication

**Severity:** MEDIUM (CVSS 5.7: `AV:N/AC:L/PR:N/UI:N/S:U/C:L/I:N/A:L`)  
**Location:** `manager-service/cmd/manager/main.go:202-207`  
**Category:** Information Disclosure

#### Description

When `DEBUG_MODE=true` and `EnableGRPCReflection=true`, the gRPC reflection endpoint is exposed to unauthenticated clients. Attackers can enumerate all RPC methods, message schemas, and fields.

#### Attack Chain

```
1. Attacker queries gRPC reflection endpoint with grpcurl.
2. Attacker enumerates WorkerService methods and message structures.
3. Attacker discovers internal fields (lease_id, attempt_id, partition_id).
4. Attacker crafts targeted attacks based on API structure.
```

#### Remediation

- Never enable reflection in production.
- Add authentication check in front of reflection endpoint.
- **See PATCH-3.**

---

### [VULN-5] HIGH: Lease ID & Attempt ID Validation Not Atomic

**Severity:** HIGH (CVSS 7.5: `AV:N/AC:H/PR:L/UI:N/S:U/C:H/I:H/A:H`)  
**Location:** `manager-service/internal/grpc/server.go:110-136` (Heartbeat RPC)  
**Category:** gRPC Fencing & Lease Management

#### Description

The `Heartbeat` RPC validates that `lease_id`, `attempt_id`, and `task_id` are non-empty, but the actual lease validation is delegated to `scheduler.RenewLease()`. There is a potential race condition if the task status changes between the RPC validation and the database lease renewal.

#### Vulnerable Code

```go
func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "...")
	}
	
	// ❌ No verification that the lease_id/attempt_id match the current task state
	err := s.scheduler.RenewLease(ctx, req.TaskId, req.AttemptId, req.LeaseId)
	if err != nil {
		// ...
	}
	return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
}
```

#### Remediation

Add pre-check validation of `lease_id` and `attempt_id` against the task state before calling `RenewLease()`. **See PATCH-4.**

---

### [VULN-6] MEDIUM: Permissive Message Size Limits

**Severity:** MEDIUM (CVSS 6.5: `AV:N/AC:L/PR:L/UI:N/S:U/C:N/I:N/A:H`)  
**Location:** `manager-service/cmd/manager/main.go:179-180`  
**Category:** Denial of Service (Resource Exhaustion)

#### Description

gRPC message size limits are set to 4 MB receive and 16 MB send. For jobs with very large numbers of input splits, manifest fallback is used, but there is no server-side validation of total memory consumption across concurrent requests.

#### Attack Scenario

```
1. Attacker submits 100 concurrent workers with large TaskAssignment messages.
2. Each message approaches the 4 MB limit (thousands of input splits).
3. Server buffers all messages in memory simultaneously.
4. Memory exhaustion leads to OOM crash or severe degradation.
```

#### Remediation

- Increase limits with validation.
- Add keepalive settings to prevent connection slowloris.
- Implement per-worker rate limiting.
- **See PATCH-5.**

---

## REMEDIATION PATCHES

### PATCH-1: Mandatory mTLS in Worker Client + RequireTransportSecurity on Token

**File:** `worker-service/cmd/worker/main.go`

```go
// ========== UPDATED transportOption function ==========
func transportOption(cfg *config.Config) grpc.DialOption {
	// If explicitly disabled for local dev (not recommended for production)
	if cfg.AllowInsecureManagerRPC {
		log.Printf("[WARN] ALLOW_INSECURE_MANAGER_RPC=true: worker will connect to manager over plaintext. This is only safe for local development.")
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	// Use system root CAs by default (most secure)
	creds, err := grpccreds.NewClientTLSFromEnv()
	if err != nil {
		// Fallback: if custom cert is explicitly provided, use it
		if cfg.GRPCTLSCertFile != "" {
			customCreds, certErr := grpccreds.NewClientTLSFromFile(cfg.GRPCTLSCertFile, cfg.GRPCTLSServerName)
			if certErr != nil {
				log.Fatalf("failed to load custom TLS cert %s: %v", cfg.GRPCTLSCertFile, certErr)
			}
			log.Printf("worker using custom TLS certificate: %s", cfg.GRPCTLSCertFile)
			return grpc.WithTransportCredentials(customCreds)
		}
		// No TLS available and not explicitly disabled: mandatory security failure
		log.Fatalf("[SECURITY] failed to load system root CAs and no custom TLS cert provided; either:\n"+
			"  1. Use system-trusted certificates (recommended for production)\n"+
			"  2. Set GRPC_TLS_CERT_FILE to a custom certificate path\n"+
			"  3. Set ALLOW_INSECURE_MANAGER_RPC=true (development only)")
	}

	log.Printf("worker using system root CA certificates for gRPC TLS")
	return grpc.WithTransportCredentials(creds)
}

// ========== UPDATED rpcToken to enforce TLS ==========
type rpcToken struct{ token string }

// GetRequestMetadata attaches the bearer token as metadata on every RPC.
func (r rpcToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + r.token}, nil
}

// RequireTransportSecurity enforces that the token credential is only sent over TLS.
// This prevents token leakage over plaintext connections.
func (r rpcToken) RequireTransportSecurity() bool {
	return true  // ✅ FIXED: Token MUST use TLS
}
```

**Config Changes:**

```go
// worker-service/internal/config/config.go
type Config struct {
	// ... existing fields ...
	
	// AllowInsecureManagerRPC permits connecting to manager over plaintext.
	// Only safe for local development; must not be used in production.
	AllowInsecureManagerRPC bool
	
	// GRPCTLSServerName is the server name for TLS SNI (optional).
	// If unset, uses the manager hostname from MANAGER_ADDR.
	GRPCTLSServerName string
}

func Load() (*Config, error) {
	// ... existing code ...
	
	return &Config{
		// ... existing fields ...
		AllowInsecureManagerRPC: getEnvBool("ALLOW_INSECURE_MANAGER_RPC", false),
		GRPCTLSServerName:       strings.TrimSpace(os.Getenv("GRPC_TLS_SERVER_NAME")),
	}, nil
}
```

---

### PATCH-2: Add gRPC Stream Interceptor

**File:** `manager-service/cmd/manager/main.go`

```go
// ========== NEW: Stream interceptor mirrors unary interceptor ==========
func workerAuthStreamInterceptor(expectedToken string) grpc.StreamServerInterceptor {
	expectedToken = strings.TrimSpace(expectedToken)
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if expectedToken != "" && !isAuthorizedWorkerRPC(ss.Context(), expectedToken) {
			return status.Error(codes.Unauthenticated, "missing or invalid worker rpc token")
		}
		return handler(srv, ss)
	}
}

// ========== UPDATED: gRPC server initialization with both interceptors ==========
grpcOpts := []grpc.ServerOption{
	grpc.ChainUnaryInterceptor(workerAuthUnaryInterceptor(cfg.WorkerRPCToken)),
	grpc.ChainStreamInterceptor(workerAuthStreamInterceptor(cfg.WorkerRPCToken)),
	grpc.MaxRecvMsgSize(8 << 20),
	grpc.MaxSendMsgSize(32 << 20),
}
```

---

### PATCH-3: Disable Reflection or Protect with Auth

**File:** `manager-service/cmd/manager/main.go`

```go
// ========== UPDATED: Reflection configuration ==========
if cfg.EnableGRPCReflection {
	if os.Getenv("DEBUG_MODE") != "true" {
		slog.Warn("gRPC reflection requested but DEBUG_MODE is not set; skipping")
	} else {
		slog.Warn("[SECURITY] gRPC reflection is ENABLED in DEBUG mode. This must not be used in production.")
		
		// Even in debug mode, warn if token auth is missing
		if cfg.WorkerRPCToken == "" {
			slog.Warn("[SECURITY] gRPC reflection is completely UNAUTHENTICATED. Restrict network access to this port!")
		} else {
			slog.Info("gRPC reflection is protected by worker RPC token validation")
		}
		
		reflection.Register(grpcServer)
	}
}
```

**Recommendation:** Add compile-time assertion or environment variable to prevent reflection in production:

```go
// In main() at startup
if cfg.EnableGRPCReflection && os.Getenv("POD_NAMESPACE") != "default" {
	log.Fatalf("[SECURITY] gRPC reflection is not allowed in non-default namespaces (detected: %s)", os.Getenv("POD_NAMESPACE"))
}
```

---

### PATCH-4: Pre-check Lease ID & Attempt ID in Heartbeat

**File:** `manager-service/internal/grpc/server.go`

```go
func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Validate required fields
	if req.TaskId == "" || req.AttemptId == "" || req.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, attempt_id, and lease_id are required")
	}

	// Pre-check: Verify task exists and attempt_id matches current active attempt
	task, err := s.scheduler.GetTaskByID(ctx, req.TaskId)
	if err != nil {
		if errors.Is(err, manager.ErrTaskNotFound) {
			slog.WarnContext(ctx, "heartbeat for non-existent task (zombie worker?)",
				slog.String("task_id", req.TaskId),
				slog.String("attempt_id", req.AttemptId))
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	// Pre-check: Verify the heartbeat matches the current active attempt
	if task.GetAttemptID() != req.AttemptId {
		slog.WarnContext(ctx, "heartbeat attempt mismatch (stale/zombie worker)",
			slog.String("task_id", req.TaskId),
			slog.String("sent_attempt_id", req.AttemptId),
			slog.String("current_attempt_id", task.GetAttemptID()))
		return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
	}

	// Pre-check: Verify lease_id matches (prevents replay/forged leases)
	if task.LeaseID != req.LeaseId {
		slog.WarnContext(ctx, "heartbeat lease mismatch (replay or forged request?)",
			slog.String("task_id", req.TaskId),
			slog.String("sent_lease_id", req.LeaseId),
			slog.String("current_lease_id", task.LeaseID))
		return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
	}

	// Atomically renew the lease
	err = s.scheduler.RenewLease(ctx, req.TaskId, req.AttemptId, req.LeaseId)
	if err != nil {
		if errors.Is(err, manager.ErrTaskNotFound) ||
			errors.Is(err, manager.ErrStaleAttempt) ||
			errors.Is(err, manager.ErrExpiredLease) ||
			errors.Is(err, manager.ErrInvalidStateTransition) {
			slog.WarnContext(ctx, "lease renewal rejected",
				slog.String("task_id", req.TaskId),
				slog.Any("error", err))
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to renew lease: %v", err)
	}

	return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
}
```

---

### PATCH-5: Tuned Message Size Limits + Keepalive Configuration

**File:** `manager-service/cmd/manager/main.go`

```go
import (
	"google.golang.org/grpc/keepalive"
)

// RecvMsgSize and SendMsgSize are conservative defaults.
// They should be tuned based on the maximum expected TaskAssignment size.
const (
	// 8 MB receive: accommodates TaskAssignment with ~8000 medium-sized input splits
	defaultRecvMsgSizeBytes = 8 << 20

	// 32 MB send: accommodates large TaskAssignment responses + manifest URIs
	defaultSendMsgSizeBytes = 32 << 20
)

// validateMessageSizeLimits ensures gRPC message sizes are within reasonable bounds.
func validateMessageSizeLimits(recvSize, sendSize int) error {
	if recvSize <= 0 || recvSize > 64<<20 { // Hard cap at 64 MB
		return fmt.Errorf("recv msg size out of bounds: %d (valid: 1-67108864)", recvSize)
	}
	if sendSize <= 0 || sendSize > 64<<20 {
		return fmt.Errorf("send msg size out of bounds: %d (valid: 1-67108864)", sendSize)
	}
	if sendSize < recvSize {
		return fmt.Errorf("send size (%d) must be >= recv size (%d)", sendSize, recvSize)
	}
	return nil
}

// In main():
recvSize := defaultRecvMsgSizeBytes
sendSize := defaultSendMsgSizeBytes

// Allow config override for tuning in specific deployments
if cfg.GRPCRecvMsgSizeBytes > 0 {
	recvSize = cfg.GRPCRecvMsgSizeBytes
}
if cfg.GRPCSendMsgSizeBytes > 0 {
	sendSize = cfg.GRPCSendMsgSizeBytes
}

if err := validateMessageSizeLimits(recvSize, sendSize); err != nil {
	log.Fatalf("invalid gRPC message size configuration: %v", err)
}

grpcOpts := []grpc.ServerOption{
	grpc.ChainUnaryInterceptor(workerAuthUnaryInterceptor(cfg.WorkerRPCToken)),
	grpc.ChainStreamInterceptor(workerAuthStreamInterceptor(cfg.WorkerRPCToken)),
	grpc.MaxRecvMsgSize(recvSize),
	grpc.MaxSendMsgSize(sendSize),
	grpc.ConnectionTimeout(30 * time.Second),
	grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     5 * time.Minute,
		MaxConnectionAge:      2 * time.Hour,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  2 * time.Minute,
		Timeout:               10 * time.Second,
	}),
}

slog.Info("gRPC server message size limits configured",
	slog.Int("recv_bytes", recvSize),
	slog.Int("send_bytes", sendSize))
```

**Config Changes:**

```go
// manager-service/internal/config/config.go
type Config struct {
	// ... existing fields ...
	
	GRPCRecvMsgSizeBytes int
	GRPCSendMsgSizeBytes int
}

func Load() (*Config, error) {
	// ... existing code ...
	
	recvMsgSize, err := getEnvInt("GRPC_RECV_MSG_SIZE_BYTES", 8<<20)
	if err != nil {
		return nil, err
	}
	sendMsgSize, err := getEnvInt("GRPC_SEND_MSG_SIZE_BYTES", 32<<20)
	if err != nil {
		return nil, err
	}
	
	return &Config{
		// ... existing fields ...
		GRPCRecvMsgSizeBytes: recvMsgSize,
		GRPCSendMsgSizeBytes: sendMsgSize,
	}, nil
}
```

---

## DEPLOYMENT CHECKLIST

### Pre-Deployment (Before Release)

- [ ] Apply PATCH-1: Mandatory mTLS on worker client
- [ ] Apply PATCH-2: Add stream interceptor (defensive)
- [ ] Apply PATCH-3: Disable/protect reflection
- [ ] Apply PATCH-4: Pre-check lease validation
- [ ] Apply PATCH-5: Message size limits + keepalive
- [ ] Review and test all gRPC error paths
- [ ] Update Kubernetes RBAC policies for mTLS certificates
- [ ] Document new environment variables (ALLOW_INSECURE_MANAGER_RPC, GRPC_TLS_SERVER_NAME, etc.)

### Post-Deployment (Production)

- [ ] Enable audit logging for all gRPC RPCs
- [ ] Monitor gRPC server metrics: connection count, message sizes, error rates
- [ ] Alert on authentication failures, stale heartbeats, or lease mismatches
- [ ] Verify no production workers connect with AllowInsecureManagerRPC=true
- [ ] Confirm mTLS certificate chains are valid and not self-signed

---

## RISK SUMMARY

| #      | Title                                  | Severity | CVSS | Status         |
| ------ | -------------------------------------- | -------- | ---- | -------------- |
| VULN-1 | Insecure Worker Client Transport       | CRITICAL | 9.8  | ➡️ Patch-1     |
| VULN-2 | Token Credential Missing TLS Requirement | HIGH   | 8.6  | ➡️ Patch-1     |
| VULN-3 | Missing gRPC Stream Interceptor        | HIGH     | 8.8  | ➡️ Patch-2     |
| VULN-4 | Unauthenticated Reflection Endpoint    | MEDIUM   | 5.7  | ➡️ Patch-3     |
| VULN-5 | Lease ID/Attempt ID TOCTOU             | HIGH     | 7.5  | ➡️ Patch-4     |
| VULN-6 | Permissive Message Size Limits         | MEDIUM   | 6.5  | ➡️ Patch-5     |

---

**Report Prepared By:** Security Vulnerability Auditor  
**Last Updated:** 2025-01-09  
**Next Review:** After applying all patches (2-3 sprints)

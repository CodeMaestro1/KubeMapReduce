# Graph Report - .  (2026-04-19)

## Corpus Check
- Corpus is ~49,094 words - fits in a single context window. You may not need a graph.

## Summary
- 717 nodes · 945 edges · 59 communities detected
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.86)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_mapreduce.pb.go|mapreduce.pb.go]]
- [[_COMMUNITY_scheduler test.go|scheduler test.go]]
- [[_COMMUNITY_handlers test.go|handlers test.go]]
- [[_COMMUNITY_Scheduler|Scheduler]]
- [[_COMMUNITY_task tracker.go|task tracker.go]]
- [[_COMMUNITY_keycloakBootstrapper|keycloakBootstrapper]]
- [[_COMMUNITY_mapreduce grpc.pb.go|mapreduce grpc.pb.go]]
- [[_COMMUNITY_TaskAssignment|TaskAssignment]]
- [[_COMMUNITY_bootstrap test.go|bootstrap test.go]]
- [[_COMMUNITY_server test.go|server test.go]]
- [[_COMMUNITY_KubeMapReduce Platform|KubeMapReduce Platform]]
- [[_COMMUNITY_Handlers|Handlers]]
- [[_COMMUNITY_main()|main()]]
- [[_COMMUNITY_KeycloakAdminClient|KeycloakAdminClient]]
- [[_COMMUNITY_MemoryJobStore|MemoryJobStore]]
- [[_COMMUNITY_http.go|http.go]]
- [[_COMMUNITY_dds.go|dds.go]]
- [[_COMMUNITY_http test.go|http test.go]]
- [[_COMMUNITY_server.go|server.go]]
- [[_COMMUNITY_jwt test.go|jwt test.go]]
- [[_COMMUNITY_oauth test.go|oauth test.go]]
- [[_COMMUNITY_roles test.go|roles test.go]]
- [[_COMMUNITY_jobs.go|jobs.go]]
- [[_COMMUNITY_orchestrator.go|orchestrator.go]]
- [[_COMMUNITY_store test.go|store test.go]]
- [[_COMMUNITY_config test.go|config test.go]]
- [[_COMMUNITY_task tracker test.go|task tracker test.go]]
- [[_COMMUNITY_validation.go|validation.go]]
- [[_COMMUNITY_jobs test.go|jobs test.go]]
- [[_COMMUNITY_main test.go|main test.go]]
- [[_COMMUNITY_dds test.go|dds test.go]]
- [[_COMMUNITY_requests.go|requests.go]]
- [[_COMMUNITY_requests test.go|requests test.go]]
- [[_COMMUNITY_response test.go|response test.go]]
- [[_COMMUNITY_admin.go|admin.go]]
- [[_COMMUNITY_whoami.go|whoami.go]]
- [[_COMMUNITY_http helpers.go|http helpers.go]]
- [[_COMMUNITY_keycloak admin test.go|keycloak admin test.go]]
- [[_COMMUNITY_errors.go|errors.go]]
- [[_COMMUNITY_oauth.go|oauth.go]]
- [[_COMMUNITY_roles.go|roles.go]]
- [[_COMMUNITY_config.go|config.go]]
- [[_COMMUNITY_validation test.go|validation test.go]]
- [[_COMMUNITY_jwt.go|jwt.go]]
- [[_COMMUNITY_main test.go|main test.go]]
- [[_COMMUNITY_admin test.go|admin test.go]]
- [[_COMMUNITY_login.go|login.go]]
- [[_COMMUNITY_whoami test.go|whoami test.go]]
- [[_COMMUNITY_orchestrator test.go|orchestrator test.go]]
- [[_COMMUNITY_response.go|response.go]]
- [[_COMMUNITY_DELETE Username in Path Design|DELETE Username in Path Design]]
- [[_COMMUNITY_routes.go|routes.go]]
- [[_COMMUNITY_routes contract test.go|routes contract test.go]]
- [[_COMMUNITY_routing.go|routing.go]]
- [[_COMMUNITY_routing test.go|routing test.go]]
- [[_COMMUNITY_DELETE Body Compatibility Rationale|DELETE Body Compatibility Rationale]]
- [[_COMMUNITY_Per operation Timeout Budgeting Rationale|Per operation Timeout Budgeting Rationale]]
- [[_COMMUNITY_Defense in Depth for Internal|Defense in Depth for Internal]]
- [[_COMMUNITY_queries.go|queries.go]]

## God Nodes (most connected - your core abstractions)
1. `setupMockDB()` - 38 edges
2. `newTestHandlers()` - 31 edges
3. `Scheduler` - 25 edges
4. `TaskAssignment` - 21 edges
5. `setupMockServer()` - 15 edges
6. `keycloakBootstrapper` - 13 edges
7. `expectLeaseValidation()` - 13 edges
8. `main()` - 12 edges
9. `Handlers` - 11 edges
10. `newTestHandlersWithKeycloak()` - 11 edges

## Surprising Connections (you probably didn't know these)
- `Keycloak Authentication and RBAC` --semantically_similar_to--> `Keycloak as Identity Manager Candidate`  [INFERRED] [semantically similar]
  README.md → mapreduce.pdf
- `KubeMapReduce Platform` --conceptually_related_to--> `Distributed MapReduce Project Specification`  [INFERRED]
  README.md → mapreduce.pdf
- `Startup Recovery Graceful Degradation` --conceptually_related_to--> `Fault Tolerance and Recovery Requirement`  [INFERRED]
  changes.txt → mapreduce.pdf
- `KubeMapReduce Platform` --conceptually_related_to--> `Distributed MapReduce Project Specification`  [INFERRED]
  README.md → mapreduce.pdf
- `Cleanup Retry Reconciler` --conceptually_related_to--> `Fault Tolerance and Recovery Requirement`  [INFERRED]
  changes.txt → mapreduce.pdf

## Hyperedges (group relationships)
- **Platform Service Topology** — readme_kubemapreduce_platform, readme_cli_service, readme_manager_api_service, readme_keycloak_authentication [EXTRACTED 1.00]
- **Platform Service Topology** — readme_kubemapreduce_platform, readme_cli_service, readme_manager_api_service, readme_keycloak_authentication [EXTRACTED 1.00]
- **Fault Tolerance Alignment** — changes_scheduler_recover_timeout_hardening, changes_cleanup_retry_reconciler, pdf_fault_tolerance_requirement [INFERRED 0.83]

## Communities

### Community 0 - "mapreduce.pb.go"
Cohesion: 0.03
Nodes (11): file_proto_mapreduce_proto_init(), file_proto_mapreduce_proto_rawDescGZIP(), init(), Ack, HeartbeatRequest, HeartbeatResponse, HeartbeatResponse_Action, RegisterRequest (+3 more)

### Community 1 - "scheduler test.go"
Cohesion: 0.08
Nodes (48): recordingOrchestrator, spawnCall, expectLeaseValidation(), expectReduceTaskMetadataQueries(), expectTaskMetadataQueries(), setupMockDB(), setupMockDBWithOrchestrator(), TestScheduler_AllMapTasksCompleted_Negative() (+40 more)

### Community 2 - "handlers test.go"
Cohesion: 0.07
Nodes (47): fakeKeycloak(), newTestHandlers(), newTestHandlersWithKeycloak(), newTestHandlersWithRetention(), TestHandleAdminCreateUser_NilClientUnavailable(), TestHandleAdminCreateUser_NormalizesRole(), TestHandleAdminCreateUser_RejectsInvalidJSON(), TestHandleAdminCreateUser_RejectsInvalidRole() (+39 more)

### Community 3 - "Scheduler"
Cohesion: 0.12
Nodes (2): Scheduler, taskMetadataQuerier

### Community 4 - "task tracker.go"
Cohesion: 0.08
Nodes (13): JobRecord, JobState, ScheduleJobRequest, ScheduleTask, ScheduleTaskInput, SystemConfigUpdate, Task, TaskInputSplit (+5 more)

### Community 5 - "keycloakBootstrapper"
Cohesion: 0.21
Nodes (9): BootstrapConfig, keycloakBootstrapper, keycloakClient, tokenResponse, BootstrapKeycloak(), BootstrapKeycloakWithContext(), newBootstrapHTTPClient(), newBootstrapTransport() (+1 more)

### Community 6 - "mapreduce grpc.pb.go"
Cohesion: 0.12
Nodes (9): RegisterWorkerServiceServer(), _WorkerService_Heartbeat_Handler(), _WorkerService_Register_Handler(), _WorkerService_TaskComplete_Handler(), _WorkerService_TaskFailed_Handler(), UnimplementedWorkerServiceServer, UnsafeWorkerServiceServer, WorkerServiceClient (+1 more)

### Community 7 - "TaskAssignment"
Cohesion: 0.1
Nodes (1): TaskAssignment

### Community 8 - "bootstrap test.go"
Cohesion: 0.12
Nodes (5): roundTripFunc, TestBootstrapKeycloakWithContextCanceled(), TestCallJSONRetriesNetworkErrorsAndReturnsServiceUnavailable(), TestNewKeycloakBootstrapperUsesDefaultHTTPClientTimeouts(), validBootstrapConfig()

### Community 9 - "server test.go"
Cohesion: 0.2
Nodes (16): fakeManifestUploader, setupMockServer(), TestWorkerServer_Heartbeat_Expired(), TestWorkerServer_Heartbeat_Success(), TestWorkerServer_Register_ManifestFallback(), TestWorkerServer_Register_ManifestUploadFailureReturnsError(), TestWorkerServer_Register_PermissionDenied(), TestWorkerServer_Register_ReduceUsesReplicaPartition() (+8 more)

### Community 10 - "KubeMapReduce Platform"
Cohesion: 0.16
Nodes (18): Cleanup Retry Reconciler, Eventual Consistency Reconciler Rationale, Graceful Degradation at Startup Rationale, Startup Recovery Graceful Degradation, Fault Tolerance Alignment, Platform Service Topology, Distributed MapReduce Project Specification, Fault Tolerance and Recovery Requirement (+10 more)

### Community 11 - "Handlers"
Cohesion: 0.15
Nodes (4): Handlers, isAuthDependencyError(), jobMessage(), parsePagination()

### Community 12 - "main()"
Cohesion: 0.22
Nodes (12): emitWorkerRPCSecurityWarnings(), getEnv(), isAuthorizedInternalCancel(), isAuthorizedWorkerRPC(), isLoopbackRemoteAddr(), main(), parseReplicaIndexFromHostname(), printUsage() (+4 more)

### Community 13 - "KeycloakAdminClient"
Cohesion: 0.25
Nodes (5): CreateUserRequest, KeycloakAdminClient, keycloakTokenResponse, keycloakUser, roleMapping

### Community 14 - "MemoryJobStore"
Cohesion: 0.17
Nodes (4): JobRecord, JobStore, MemoryJobStore, PostgresJobStore

### Community 15 - "http.go"
Cohesion: 0.31
Nodes (13): apiURL(), cliRequestContext(), doAuthRequest(), doAuthRequestExpect(), doAuthRequestWithContext(), getEnv(), getValidToken(), keycloakBaseURL() (+5 more)

### Community 16 - "dds.go"
Cohesion: 0.14
Nodes (11): AttemptStatus, Job, JobConfig, JobStatus, SystemConfig, Task, TaskAttempt, TaskInput (+3 more)

### Community 17 - "http test.go"
Cohesion: 0.17
Nodes (0): 

### Community 18 - "server.go"
Cohesion: 0.21
Nodes (6): manifestUploader, minioManifestUploader, WorkerServer, findMapSplitForTask(), NewWorkerServer(), newWorkerServerWithManifestUploader()

### Community 19 - "jwt test.go"
Cohesion: 0.24
Nodes (6): stubKeyfunc, newTestValidator(), splitAuthHeader(), TestMiddleware_InvalidAuthHeaderFormat(), TestMiddleware_InvalidToken_ReturnsGenericError(), TestMiddleware_MissingAuthHeader()

### Community 20 - "oauth test.go"
Cohesion: 0.18
Nodes (0): 

### Community 21 - "roles test.go"
Cohesion: 0.18
Nodes (0): 

### Community 22 - "jobs.go"
Cohesion: 0.27
Nodes (9): cliJobFuncSpec, cliJobPayload, cmdJobsDownload(), cmdJobsStatus(), cmdJobsSubmit(), inferLanguage(), jobRequestPath(), safeJobResultFilename() (+1 more)

### Community 23 - "orchestrator.go"
Cohesion: 0.25
Nodes (5): KubeOrchestrator, MockOrchestrator, WorkerOrchestrator, buildWorkerJobName(), sanitizeForDNSLabel()

### Community 24 - "store test.go"
Cohesion: 0.2
Nodes (0): 

### Community 25 - "config test.go"
Cohesion: 0.2
Nodes (0): 

### Community 26 - "task tracker test.go"
Cohesion: 0.2
Nodes (0): 

### Community 27 - "validation.go"
Cohesion: 0.31
Nodes (6): BadRequestError, NewBadRequestError(), NormalizeRole(), ValidateCreateUserRequest(), validateFunctionSpec(), ValidateJobSubmission()

### Community 28 - "jobs test.go"
Cohesion: 0.25
Nodes (2): testExit, TestValidateReducersCount()

### Community 29 - "main test.go"
Cohesion: 0.22
Nodes (0): 

### Community 30 - "dds test.go"
Cohesion: 0.22
Nodes (0): 

### Community 31 - "requests.go"
Cohesion: 0.22
Nodes (8): CreateUserRequest, DeleteUserRequest, FunctionSpec, JobStatusResponse, JobSubmissionRequest, JobSubmissionResponse, NodeConfigRequest, WorkerConfigRequest

### Community 32 - "requests test.go"
Cohesion: 0.22
Nodes (0): 

### Community 33 - "response test.go"
Cohesion: 0.25
Nodes (2): failingWriteResponseWriter, TestWriteJSON_Success()

### Community 34 - "admin.go"
Cohesion: 0.43
Nodes (7): cmdAdminConfigureNodes(), cmdAdminCreateUser(), cmdAdminDeleteUser(), cmdAdminWorkerConfig(), configureNodesStatusError(), requireAdminRole(), runAdminConfigureNodes()

### Community 35 - "whoami.go"
Cohesion: 0.46
Nodes (7): cmdTokenInspect(), cmdWhoAmI(), decodeTokenClaims(), extractRealmRoles(), hasAdminRole(), hasRealmRole(), loadTokensAndClaims()

### Community 36 - "http helpers.go"
Cohesion: 0.29
Nodes (0): 

### Community 37 - "keycloak admin test.go"
Cohesion: 0.29
Nodes (0): 

### Community 38 - "errors.go"
Cohesion: 0.33
Nodes (1): ServiceUnavailableError

### Community 39 - "oauth.go"
Cohesion: 0.47
Nodes (5): OAuthTokenResponse, RefreshTokens(), RefreshTokensWithContext(), RequestTokens(), RequestTokensWithContext()

### Community 40 - "roles.go"
Cohesion: 0.6
Nodes (5): containsRole(), GetRoles(), RequireAnyRole(), RequireRole(), requireRoles()

### Community 41 - "config.go"
Cohesion: 0.67
Nodes (5): Config, getEnv(), getEnvBool(), getEnvInt(), Load()

### Community 42 - "validation test.go"
Cohesion: 0.4
Nodes (2): TestValidateJobSubmission(), validJobSubmissionRequest()

### Community 43 - "jwt.go"
Cohesion: 0.4
Nodes (2): contextKey, JWTValidator

### Community 44 - "main test.go"
Cohesion: 0.5
Nodes (0): 

### Community 45 - "admin test.go"
Cohesion: 0.5
Nodes (0): 

### Community 46 - "login.go"
Cohesion: 0.5
Nodes (0): 

### Community 47 - "whoami test.go"
Cohesion: 0.5
Nodes (0): 

### Community 48 - "orchestrator test.go"
Cohesion: 0.5
Nodes (0): 

### Community 49 - "response.go"
Cohesion: 1.0
Nodes (2): WriteError(), WriteJSON()

### Community 50 - "DELETE Username in Path Design"
Cohesion: 0.67
Nodes (3): DELETE /admin/users/{username} Endpoint, DELETE Body Compatibility Rationale, DELETE Username in Path Design

### Community 51 - "routes.go"
Cohesion: 1.0
Nodes (0): 

### Community 52 - "routes contract test.go"
Cohesion: 1.0
Nodes (0): 

### Community 53 - "routing.go"
Cohesion: 1.0
Nodes (0): 

### Community 54 - "routing test.go"
Cohesion: 1.0
Nodes (0): 

### Community 55 - "DELETE Body Compatibility Rationale"
Cohesion: 1.0
Nodes (2): DELETE Body Compatibility Rationale, DELETE Username in Path Design

### Community 56 - "Per operation Timeout Budgeting Rationale"
Cohesion: 1.0
Nodes (2): Per-operation Timeout Budgeting Rationale, Scheduler Recover Timeout Hardening

### Community 57 - "Defense in Depth for Internal"
Cohesion: 1.0
Nodes (2): Defense in Depth for Internal Control Planes, Internal Job Cancel Endpoint Auth

### Community 58 - "queries.go"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **57 isolated node(s):** `BootstrapConfig`, `tokenResponse`, `keycloakClient`, `contextKey`, `CreateUserRequest` (+52 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `routes.go`** (2 nodes): `routes.go`, `RegisterRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `routes contract test.go`** (2 nodes): `routes_contract_test.go`, `TestCLIAdminRoutes_MatchAPIRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `routing.go`** (2 nodes): `routing.go`, `ComputeReplicaIndex()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `routing test.go`** (2 nodes): `routing_test.go`, `TestComputeReplicaIndex()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `DELETE Body Compatibility Rationale`** (2 nodes): `DELETE Body Compatibility Rationale`, `DELETE Username in Path Design`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Per operation Timeout Budgeting Rationale`** (2 nodes): `Per-operation Timeout Budgeting Rationale`, `Scheduler Recover Timeout Hardening`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Defense in Depth for Internal`** (2 nodes): `Defense in Depth for Internal Control Planes`, `Internal Job Cancel Endpoint Auth`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `queries.go`** (1 nodes): `queries.go`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `TaskAssignment` connect `TaskAssignment` to `mapreduce.pb.go`?**
  _High betweenness centrality (0.006) - this node is a cross-community bridge._
- **What connects `BootstrapConfig`, `tokenResponse`, `keycloakClient` to the rest of the system?**
  _57 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `mapreduce.pb.go` be split into smaller, more focused modules?**
  _Cohesion score 0.03 - nodes in this community are weakly interconnected._
- **Should `scheduler test.go` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `handlers test.go` be split into smaller, more focused modules?**
  _Cohesion score 0.07 - nodes in this community are weakly interconnected._
- **Should `Scheduler` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
- **Should `task tracker.go` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
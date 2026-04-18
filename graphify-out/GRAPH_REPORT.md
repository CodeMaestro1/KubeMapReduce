# Graph Report - .  (2026-04-19)

## Corpus Check
- Corpus is ~46,209 words - fits in a single context window. You may not need a graph.

## Summary
- 678 nodes · 898 edges · 56 communities detected
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS · INFERRED: 4 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Descriptor String|Descriptor String]]
- [[_COMMUNITY_Testscheduler Completetask|Testscheduler Completetask]]
- [[_COMMUNITY_Testhandleadmincreateuser Testhandlejobssubmit|Testhandleadmincreateuser Testhandlejobssubmit]]
- [[_COMMUNITY_Scheduler Allmaptaskscompleted|Scheduler Allmaptaskscompleted]]
- [[_COMMUNITY_String Jobrecord|String Jobrecord]]
- [[_COMMUNITY_Bootstrap Bootstrapconfig|Bootstrap Bootstrapconfig]]
- [[_COMMUNITY_Workerservice Handler|Workerservice Handler]]
- [[_COMMUNITY_Taskassignment Getattemptid|Taskassignment Getattemptid]]
- [[_COMMUNITY_Roundtripfunc Roundtrip|Roundtripfunc Roundtrip]]
- [[_COMMUNITY_Testworkerserver Register|Testworkerserver Register]]
- [[_COMMUNITY_Handlers Cleanupjobsstore|Handlers Cleanupjobsstore]]
- [[_COMMUNITY_Createuserrequest Keycloakadminclient|Createuserrequest Keycloakadminclient]]
- [[_COMMUNITY_Http Apiurl|Http Apiurl]]
- [[_COMMUNITY_Getenv Isauthorizedinternalcancel|Getenv Isauthorizedinternalcancel]]
- [[_COMMUNITY_Dds Attemptstatus|Dds Attemptstatus]]
- [[_COMMUNITY_Testdoauthrequestwithcontext Testgetvalidtoken|Testdoauthrequestwithcontext Testgetvalidtoken]]
- [[_COMMUNITY_Manifestuploader Miniomanifestuploader|Manifestuploader Miniomanifestuploader]]
- [[_COMMUNITY_Reconciler Requirement|Reconciler Requirement]]
- [[_COMMUNITY_Testmiddleware Jwt|Testmiddleware Jwt]]
- [[_COMMUNITY_Testrequesttokens Testrefreshtokens|Testrequesttokens Testrefreshtokens]]
- [[_COMMUNITY_Testgetroles Testrequireroles|Testgetroles Testrequireroles]]
- [[_COMMUNITY_Clijobfuncspec Clijobpayload|Clijobfuncspec Clijobpayload]]
- [[_COMMUNITY_Canceljob Spawnworker|Canceljob Spawnworker]]
- [[_COMMUNITY_Testload Customenvvars|Testload Customenvvars]]
- [[_COMMUNITY_String Testnewtasktracker|String Testnewtasktracker]]
- [[_COMMUNITY_Validation Badrequesterror|Validation Badrequesterror]]
- [[_COMMUNITY_Testcmdjobssubmit Testsafejobresultfilename|Testcmdjobssubmit Testsafejobresultfilename]]
- [[_COMMUNITY_Serialization Testtaskattempt|Serialization Testtaskattempt]]
- [[_COMMUNITY_Requests Createuserrequest|Requests Createuserrequest]]
- [[_COMMUNITY_Json Testjobsubmissionrequest|Json Testjobsubmissionrequest]]
- [[_COMMUNITY_Testwritejson Failingwriteresponsewriter|Testwritejson Failingwriteresponsewriter]]
- [[_COMMUNITY_Cmdadminconfigurenodes Cmdadmincreateuser|Cmdadminconfigurenodes Cmdadmincreateuser]]
- [[_COMMUNITY_Whoami Cmdtokeninspect|Whoami Cmdtokeninspect]]
- [[_COMMUNITY_Testisauthorizedinternalcancel Testisauthorizedworkerrpc|Testisauthorizedinternalcancel Testisauthorizedworkerrpc]]
- [[_COMMUNITY_Http Helpers|Http Helpers]]
- [[_COMMUNITY_Testgetadminaccesstoken Keycloak|Testgetadminaccesstoken Keycloak]]
- [[_COMMUNITY_Errors Serviceunavailableerror|Errors Serviceunavailableerror]]
- [[_COMMUNITY_Oauthtokenresponse Oauth|Oauthtokenresponse Oauth]]
- [[_COMMUNITY_Roles Containsrole|Roles Containsrole]]
- [[_COMMUNITY_Getenv Getenvbool|Getenv Getenvbool]]
- [[_COMMUNITY_Validation Testbadrequesthelpers|Validation Testbadrequesthelpers]]
- [[_COMMUNITY_Contextkey Jwtvalidator|Contextkey Jwtvalidator]]
- [[_COMMUNITY_Testbootstrapphasetimeout Testphasesuseindependentcontexts|Testbootstrapphasetimeout Testphasesuseindependentcontexts]]
- [[_COMMUNITY_Testconfigurenodesstatuserror Acceptedisnil|Testconfigurenodesstatuserror Acceptedisnil]]
- [[_COMMUNITY_Login Cmdhealth|Login Cmdhealth]]
- [[_COMMUNITY_Caseinsensitive Testhasrealmrole|Caseinsensitive Testhasrealmrole]]
- [[_COMMUNITY_Testkubeorchestrator Spawnworker|Testkubeorchestrator Spawnworker]]
- [[_COMMUNITY_Response Writeerror|Response Writeerror]]
- [[_COMMUNITY_Routes Registerroutes|Routes Registerroutes]]
- [[_COMMUNITY_Routes Contract|Routes Contract]]
- [[_COMMUNITY_Routing Computereplicaindex|Routing Computereplicaindex]]
- [[_COMMUNITY_Routing Testcomputereplicaindex|Routing Testcomputereplicaindex]]
- [[_COMMUNITY_Graceful Degradation|Graceful Degradation]]
- [[_COMMUNITY_Timeout Per|Timeout Per]]
- [[_COMMUNITY_Delete Body|Delete Body]]
- [[_COMMUNITY_Queries|Queries]]

## God Nodes (most connected - your core abstractions)
1. `setupMockDB()` - 38 edges
2. `newTestHandlers()` - 29 edges
3. `Scheduler` - 25 edges
4. `TaskAssignment` - 21 edges
5. `setupMockServer()` - 15 edges
6. `keycloakBootstrapper` - 13 edges
7. `expectLeaseValidation()` - 13 edges
8. `Handlers` - 12 edges
9. `newTestHandlersWithKeycloak()` - 11 edges
10. `TaskCompleteRequest` - 11 edges

## Surprising Connections (you probably didn't know these)
- `Keycloak Authentication and RBAC` --semantically_similar_to--> `Keycloak as Identity Manager Candidate`  [INFERRED] [semantically similar]
  README.md → mapreduce.pdf
- `Cleanup Retry Reconciler` --conceptually_related_to--> `Fault Tolerance and Recovery Requirement`  [INFERRED]
  changes.txt → mapreduce.pdf
- `KubeMapReduce Platform` --conceptually_related_to--> `Distributed MapReduce Project Specification`  [INFERRED]
  README.md → mapreduce.pdf
- `main()` --calls--> `getEnv()`  [EXTRACTED]
  manager-service\cmd\manager\main.go → auth-service\cmd\setup\main.go
- `main()` --calls--> `printUsage()`  [EXTRACTED]
  manager-service\cmd\manager\main.go → cli-service\cmd\cli\main.go

## Hyperedges (group relationships)
- **Platform Service Topology** — readme_kubemapreduce_platform, readme_cli_service, readme_manager_api_service, readme_keycloak_authentication [EXTRACTED 1.00]
- **Fault Tolerance Alignment** — changes_scheduler_recover, changes_cleanup_reconciler, pdf_fault_tolerance_requirement [INFERRED 0.83]

## Communities

### Community 0 - "Descriptor String"
Cohesion: 0.03
Nodes (11): file_proto_mapreduce_proto_init(), file_proto_mapreduce_proto_rawDescGZIP(), init(), Ack, HeartbeatRequest, HeartbeatResponse, HeartbeatResponse_Action, RegisterRequest (+3 more)

### Community 1 - "Testscheduler Completetask"
Cohesion: 0.08
Nodes (48): recordingOrchestrator, spawnCall, expectLeaseValidation(), expectReduceTaskMetadataQueries(), expectTaskMetadataQueries(), setupMockDB(), setupMockDBWithOrchestrator(), TestScheduler_AllMapTasksCompleted_Negative() (+40 more)

### Community 2 - "Testhandleadmincreateuser Testhandlejobssubmit"
Cohesion: 0.08
Nodes (45): fakeKeycloak(), newTestHandlers(), newTestHandlersWithKeycloak(), newTestHandlersWithRetention(), TestHandleAdminCreateUser_NilClientUnavailable(), TestHandleAdminCreateUser_NormalizesRole(), TestHandleAdminCreateUser_RejectsInvalidJSON(), TestHandleAdminCreateUser_RejectsInvalidRole() (+37 more)

### Community 3 - "Scheduler Allmaptaskscompleted"
Cohesion: 0.12
Nodes (2): Scheduler, taskMetadataQuerier

### Community 4 - "String Jobrecord"
Cohesion: 0.08
Nodes (13): JobRecord, JobState, ScheduleJobRequest, ScheduleTask, ScheduleTaskInput, SystemConfigUpdate, Task, TaskInputSplit (+5 more)

### Community 5 - "Bootstrap Bootstrapconfig"
Cohesion: 0.21
Nodes (9): BootstrapConfig, keycloakBootstrapper, keycloakClient, tokenResponse, BootstrapKeycloak(), BootstrapKeycloakWithContext(), newBootstrapHTTPClient(), newBootstrapTransport() (+1 more)

### Community 6 - "Workerservice Handler"
Cohesion: 0.12
Nodes (9): RegisterWorkerServiceServer(), _WorkerService_Heartbeat_Handler(), _WorkerService_Register_Handler(), _WorkerService_TaskComplete_Handler(), _WorkerService_TaskFailed_Handler(), UnimplementedWorkerServiceServer, UnsafeWorkerServiceServer, WorkerServiceClient (+1 more)

### Community 7 - "Taskassignment Getattemptid"
Cohesion: 0.1
Nodes (1): TaskAssignment

### Community 8 - "Roundtripfunc Roundtrip"
Cohesion: 0.12
Nodes (5): roundTripFunc, TestBootstrapKeycloakWithContextCanceled(), TestCallJSONRetriesNetworkErrorsAndReturnsServiceUnavailable(), TestNewKeycloakBootstrapperUsesDefaultHTTPClientTimeouts(), validBootstrapConfig()

### Community 9 - "Testworkerserver Register"
Cohesion: 0.2
Nodes (16): fakeManifestUploader, setupMockServer(), TestWorkerServer_Heartbeat_Expired(), TestWorkerServer_Heartbeat_Success(), TestWorkerServer_Register_ManifestFallback(), TestWorkerServer_Register_ManifestUploadFailureReturnsError(), TestWorkerServer_Register_PermissionDenied(), TestWorkerServer_Register_ReduceUsesReplicaPartition() (+8 more)

### Community 10 - "Handlers Cleanupjobsstore"
Cohesion: 0.18
Nodes (5): Handlers, generateJobID(), isAuthDependencyError(), NewHandlers(), newHandlersWithOptions()

### Community 11 - "Createuserrequest Keycloakadminclient"
Cohesion: 0.25
Nodes (5): CreateUserRequest, KeycloakAdminClient, keycloakTokenResponse, keycloakUser, roleMapping

### Community 12 - "Http Apiurl"
Cohesion: 0.31
Nodes (13): apiURL(), cliRequestContext(), doAuthRequest(), doAuthRequestExpect(), doAuthRequestWithContext(), getEnv(), getValidToken(), keycloakBaseURL() (+5 more)

### Community 13 - "Getenv Isauthorizedinternalcancel"
Cohesion: 0.24
Nodes (10): getEnv(), isAuthorizedInternalCancel(), isAuthorizedWorkerRPC(), isLoopbackRemoteAddr(), main(), parseReplicaIndexFromHostname(), printUsage(), resolveManagerAddr() (+2 more)

### Community 14 - "Dds Attemptstatus"
Cohesion: 0.14
Nodes (11): AttemptStatus, Job, JobConfig, JobStatus, SystemConfig, Task, TaskAttempt, TaskInput (+3 more)

### Community 15 - "Testdoauthrequestwithcontext Testgetvalidtoken"
Cohesion: 0.17
Nodes (0): 

### Community 16 - "Manifestuploader Miniomanifestuploader"
Cohesion: 0.21
Nodes (6): manifestUploader, minioManifestUploader, WorkerServer, findMapSplitForTask(), NewWorkerServer(), newWorkerServerWithManifestUploader()

### Community 17 - "Reconciler Requirement"
Cohesion: 0.2
Nodes (12): Cleanup Retry Reconciler, Eventual Consistency Reconciler Rationale, Internal Job Cancel Endpoint Auth, Fault Tolerance and Recovery Requirement, Keycloak as Identity Manager Candidate, Kubernetes Orchestration Requirement, Distributed MapReduce Project Specification, Workers as Kubernetes Jobs (+4 more)

### Community 18 - "Testmiddleware Jwt"
Cohesion: 0.24
Nodes (6): stubKeyfunc, newTestValidator(), splitAuthHeader(), TestMiddleware_InvalidAuthHeaderFormat(), TestMiddleware_InvalidToken_ReturnsGenericError(), TestMiddleware_MissingAuthHeader()

### Community 19 - "Testrequesttokens Testrefreshtokens"
Cohesion: 0.18
Nodes (0): 

### Community 20 - "Testgetroles Testrequireroles"
Cohesion: 0.18
Nodes (0): 

### Community 21 - "Clijobfuncspec Clijobpayload"
Cohesion: 0.27
Nodes (9): cliJobFuncSpec, cliJobPayload, cmdJobsDownload(), cmdJobsStatus(), cmdJobsSubmit(), inferLanguage(), jobRequestPath(), safeJobResultFilename() (+1 more)

### Community 22 - "Canceljob Spawnworker"
Cohesion: 0.25
Nodes (5): KubeOrchestrator, MockOrchestrator, WorkerOrchestrator, buildWorkerJobName(), sanitizeForDNSLabel()

### Community 23 - "Testload Customenvvars"
Cohesion: 0.2
Nodes (0): 

### Community 24 - "String Testnewtasktracker"
Cohesion: 0.2
Nodes (0): 

### Community 25 - "Validation Badrequesterror"
Cohesion: 0.31
Nodes (6): BadRequestError, NewBadRequestError(), NormalizeRole(), ValidateCreateUserRequest(), validateFunctionSpec(), ValidateJobSubmission()

### Community 26 - "Testcmdjobssubmit Testsafejobresultfilename"
Cohesion: 0.25
Nodes (2): testExit, TestValidateReducersCount()

### Community 27 - "Serialization Testtaskattempt"
Cohesion: 0.22
Nodes (0): 

### Community 28 - "Requests Createuserrequest"
Cohesion: 0.22
Nodes (8): CreateUserRequest, DeleteUserRequest, FunctionSpec, JobStatusResponse, JobSubmissionRequest, JobSubmissionResponse, NodeConfigRequest, WorkerConfigRequest

### Community 29 - "Json Testjobsubmissionrequest"
Cohesion: 0.22
Nodes (0): 

### Community 30 - "Testwritejson Failingwriteresponsewriter"
Cohesion: 0.25
Nodes (2): failingWriteResponseWriter, TestWriteJSON_Success()

### Community 31 - "Cmdadminconfigurenodes Cmdadmincreateuser"
Cohesion: 0.43
Nodes (7): cmdAdminConfigureNodes(), cmdAdminCreateUser(), cmdAdminDeleteUser(), cmdAdminWorkerConfig(), configureNodesStatusError(), requireAdminRole(), runAdminConfigureNodes()

### Community 32 - "Whoami Cmdtokeninspect"
Cohesion: 0.46
Nodes (7): cmdTokenInspect(), cmdWhoAmI(), decodeTokenClaims(), extractRealmRoles(), hasAdminRole(), hasRealmRole(), loadTokensAndClaims()

### Community 33 - "Testisauthorizedinternalcancel Testisauthorizedworkerrpc"
Cohesion: 0.25
Nodes (0): 

### Community 34 - "Http Helpers"
Cohesion: 0.29
Nodes (0): 

### Community 35 - "Testgetadminaccesstoken Keycloak"
Cohesion: 0.29
Nodes (0): 

### Community 36 - "Errors Serviceunavailableerror"
Cohesion: 0.33
Nodes (1): ServiceUnavailableError

### Community 37 - "Oauthtokenresponse Oauth"
Cohesion: 0.47
Nodes (5): OAuthTokenResponse, RefreshTokens(), RefreshTokensWithContext(), RequestTokens(), RequestTokensWithContext()

### Community 38 - "Roles Containsrole"
Cohesion: 0.6
Nodes (5): containsRole(), GetRoles(), RequireAnyRole(), RequireRole(), requireRoles()

### Community 39 - "Getenv Getenvbool"
Cohesion: 0.67
Nodes (5): Config, getEnv(), getEnvBool(), getEnvInt(), Load()

### Community 40 - "Validation Testbadrequesthelpers"
Cohesion: 0.4
Nodes (2): TestValidateJobSubmission(), validJobSubmissionRequest()

### Community 41 - "Contextkey Jwtvalidator"
Cohesion: 0.4
Nodes (2): contextKey, JWTValidator

### Community 42 - "Testbootstrapphasetimeout Testphasesuseindependentcontexts"
Cohesion: 0.5
Nodes (0): 

### Community 43 - "Testconfigurenodesstatuserror Acceptedisnil"
Cohesion: 0.5
Nodes (0): 

### Community 44 - "Login Cmdhealth"
Cohesion: 0.5
Nodes (0): 

### Community 45 - "Caseinsensitive Testhasrealmrole"
Cohesion: 0.5
Nodes (0): 

### Community 46 - "Testkubeorchestrator Spawnworker"
Cohesion: 0.5
Nodes (0): 

### Community 47 - "Response Writeerror"
Cohesion: 1.0
Nodes (2): WriteError(), WriteJSON()

### Community 48 - "Routes Registerroutes"
Cohesion: 1.0
Nodes (0): 

### Community 49 - "Routes Contract"
Cohesion: 1.0
Nodes (0): 

### Community 50 - "Routing Computereplicaindex"
Cohesion: 1.0
Nodes (0): 

### Community 51 - "Routing Testcomputereplicaindex"
Cohesion: 1.0
Nodes (0): 

### Community 52 - "Graceful Degradation"
Cohesion: 1.0
Nodes (2): Graceful Degradation at Startup Rationale, Startup Recovery Graceful Degradation

### Community 53 - "Timeout Per"
Cohesion: 1.0
Nodes (2): Per-operation Timeout Budgeting Rationale, Scheduler Recover Timeout Hardening

### Community 54 - "Delete Body"
Cohesion: 1.0
Nodes (2): DELETE Body Compatibility Rationale, DELETE Username in Path Design

### Community 55 - "Queries"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **51 isolated node(s):** `BootstrapConfig`, `tokenResponse`, `keycloakClient`, `contextKey`, `CreateUserRequest` (+46 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Routes Registerroutes`** (2 nodes): `routes.go`, `RegisterRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Routes Contract`** (2 nodes): `routes_contract_test.go`, `TestCLIAdminRoutes_MatchAPIRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Routing Computereplicaindex`** (2 nodes): `routing.go`, `ComputeReplicaIndex()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Routing Testcomputereplicaindex`** (2 nodes): `routing_test.go`, `TestComputeReplicaIndex()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Graceful Degradation`** (2 nodes): `Graceful Degradation at Startup Rationale`, `Startup Recovery Graceful Degradation`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Timeout Per`** (2 nodes): `Per-operation Timeout Budgeting Rationale`, `Scheduler Recover Timeout Hardening`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Delete Body`** (2 nodes): `DELETE Body Compatibility Rationale`, `DELETE Username in Path Design`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Queries`** (1 nodes): `queries.go`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `TaskAssignment` connect `Taskassignment Getattemptid` to `Descriptor String`?**
  _High betweenness centrality (0.007) - this node is a cross-community bridge._
- **What connects `BootstrapConfig`, `tokenResponse`, `keycloakClient` to the rest of the system?**
  _51 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Descriptor String` be split into smaller, more focused modules?**
  _Cohesion score 0.03 - nodes in this community are weakly interconnected._
- **Should `Testscheduler Completetask` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `Testhandleadmincreateuser Testhandlejobssubmit` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `Scheduler Allmaptaskscompleted` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
- **Should `String Jobrecord` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
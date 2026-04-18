# Graph Report - .  (2026-04-17)

## Corpus Check
- Corpus is ~36,220 words - fits in a single context window. You may not need a graph.

## Summary
- 497 nodes · 661 edges · 44 communities detected
- Extraction: 99% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.87)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Fakekeycloak Newtesthandlers|Fakekeycloak Newtesthandlers]]
- [[_COMMUNITY_Scheduler_Test Expectleasevalidation|Scheduler_Test Expectleasevalidation]]
- [[_COMMUNITY_String Task_Tracker|String Task_Tracker]]
- [[_COMMUNITY_Requirement Workers|Requirement Workers]]
- [[_COMMUNITY_Bootstrap Bootstrapconfig|Bootstrap Bootstrapconfig]]
- [[_COMMUNITY_Scheduler Allmaptaskscompleted|Scheduler Allmaptaskscompleted]]
- [[_COMMUNITY_Roundtripfunc Roundtrip|Roundtripfunc Roundtrip]]
- [[_COMMUNITY_Handlers Cleanupjobsstore|Handlers Cleanupjobsstore]]
- [[_COMMUNITY_Createuserrequest Keycloakadminclient|Createuserrequest Keycloakadminclient]]
- [[_COMMUNITY_Dds Attemptstatus|Dds Attemptstatus]]
- [[_COMMUNITY_Apiurl Clirequestcontext|Apiurl Clirequestcontext]]
- [[_COMMUNITY_Clijobfuncspec Clijobpayload|Clijobfuncspec Clijobpayload]]
- [[_COMMUNITY_Testexit Error|Testexit Error]]
- [[_COMMUNITY_Stubkeyfunc Keyfunc|Stubkeyfunc Keyfunc]]
- [[_COMMUNITY_Testrefreshtokens_Connectionerror Testrefreshtokens_Httperror|Testrefreshtokens_Connectionerror Testrefreshtokens_Httperror]]
- [[_COMMUNITY_Roles_Test Testcontainsrole|Roles_Test Testcontainsrole]]
- [[_COMMUNITY_Http_Test Testclihttpclient_Hastimeout|Http_Test Testclihttpclient_Hastimeout]]
- [[_COMMUNITY_Task_Tracker_Test Testjobrecord_Fields|Task_Tracker_Test Testjobrecord_Fields]]
- [[_COMMUNITY_Validation Badrequesterror|Validation Badrequesterror]]
- [[_COMMUNITY_Whoami Cmdtokeninspect|Whoami Cmdtokeninspect]]
- [[_COMMUNITY_Testjob_Isterminal Testjobconfig_Serialization|Testjob_Isterminal Testjobconfig_Serialization]]
- [[_COMMUNITY_Requests Createuserrequest|Requests Createuserrequest]]
- [[_COMMUNITY_Requests_Test Testcreateuserrequest_Json|Requests_Test Testcreateuserrequest_Json]]
- [[_COMMUNITY_Failingwriteresponsewriter Header|Failingwriteresponsewriter Header]]
- [[_COMMUNITY_Cmdadminconfigurenodes Cmdadmincreateuser|Cmdadminconfigurenodes Cmdadmincreateuser]]
- [[_COMMUNITY_Testload_Customenvvars Testload_Defaults|Testload_Customenvvars Testload_Defaults]]
- [[_COMMUNITY_Testcliadminroutes_Matchapiroutes Testcmdadminconfigurenodes_Accepts202Andprintsbody|Testcliadminroutes_Matchapiroutes Testcmdadminconfigurenodes_Accepts202Andprintsbody]]
- [[_COMMUNITY_Whoami_Test Buildjwtfortest|Whoami_Test Buildjwtfortest]]
- [[_COMMUNITY_Ensurecallstatus Ensurestatus|Ensurecallstatus Ensurestatus]]
- [[_COMMUNITY_Testassignrealmrolenormalizesandescapesrolelookuppath Testassignrealmrolerejectsemptyrolenameaftertrim|Testassignrealmrolenormalizesandescapesrolelookuppath Testassignrealmrolerejectsemptyrolenameaftertrim]]
- [[_COMMUNITY_Getenv Printusage|Getenv Printusage]]
- [[_COMMUNITY_Run Defaultsetupdependencies|Run Defaultsetupdependencies]]
- [[_COMMUNITY_Run_Test Testrunsetup_Bootstrapphasetimeoutstopsflow|Run_Test Testrunsetup_Bootstrapphasetimeoutstopsflow]]
- [[_COMMUNITY_Validation_Test Testbadrequesthelpers|Validation_Test Testbadrequesthelpers]]
- [[_COMMUNITY_Serviceunavailableerror Error|Serviceunavailableerror Error]]
- [[_COMMUNITY_Oauthtokenresponse Refreshtokens|Oauthtokenresponse Refreshtokens]]
- [[_COMMUNITY_Roles Containsrole|Roles Containsrole]]
- [[_COMMUNITY_Contextkey Jwtvalidator|Contextkey Jwtvalidator]]
- [[_COMMUNITY_Login Cmdhealth|Login Cmdhealth]]
- [[_COMMUNITY_Config Getenv|Config Getenv]]
- [[_COMMUNITY_Response Writeerror|Response Writeerror]]
- [[_COMMUNITY_Routes Registerroutes|Routes Registerroutes]]
- [[_COMMUNITY_Queries|Queries]]
- [[_COMMUNITY_In-Memory Job|In-Memory Job]]

## God Nodes (most connected - your core abstractions)
1. `setupMockDB()` - 37 edges
2. `newTestHandlers()` - 29 edges
3. `Scheduler` - 18 edges
4. `keycloakBootstrapper` - 13 edges
5. `Handlers` - 12 edges
6. `expectLeaseValidation()` - 12 edges
7. `newTestHandlersWithKeycloak()` - 11 edges
8. `KeycloakAdminClient` - 9 edges
9. `Task` - 7 edges
10. `getValidToken()` - 6 edges

## Surprising Connections (you probably didn't know these)
- `CLI-Based UI Requirement` --semantically_similar_to--> `CLI Client`  [INFERRED] [semantically similar]
  mapreduce.pdf → README.md
- `Authentication Service` --semantically_similar_to--> `Keycloak`  [INFERRED] [semantically similar]
  mapreduce.pdf → README.md
- `Empty Placeholder File` --conceptually_related_to--> `KubeMapReduce Platform`  [AMBIGUOUS]
  foo.txt → README.md
- `main()` --calls--> `printUsage()`  [EXTRACTED]
  cmd\setup\main.go → cmd\cli\main.go

## Hyperedges (group relationships)
- **Admin Proxy Management Flow** — readme_admin_user_management_flow, readme_cli_client, readme_api_server, readme_keycloak [EXTRACTED 1.00]
- **Core Platform Service Group** — mapreduce_ui_service, mapreduce_manager_service, mapreduce_workers, mapreduce_distributed_data_service, mapreduce_shared_file_system, mapreduce_authentication_service [EXTRACTED 1.00]

## Communities

### Community 0 - "Fakekeycloak Newtesthandlers"
Cohesion: 0.08
Nodes (45): fakeKeycloak(), newTestHandlers(), newTestHandlersWithKeycloak(), newTestHandlersWithRetention(), TestHandleAdminCreateUser_NilClientUnavailable(), TestHandleAdminCreateUser_NormalizesRole(), TestHandleAdminCreateUser_RejectsInvalidJSON(), TestHandleAdminCreateUser_RejectsInvalidRole() (+37 more)

### Community 1 - "Scheduler_Test Expectleasevalidation"
Cohesion: 0.1
Nodes (40): expectLeaseValidation(), expectReduceTaskMetadataQueries(), expectTaskMetadataQueries(), setupMockDB(), TestScheduler_AllMapTasksCompleted_Negative(), TestScheduler_CompleteTask_AlreadyCompleted(), TestScheduler_CompleteTask_AsymmetricArrays(), TestScheduler_CompleteTask_DBClockExpiredEvenIfAppWouldThinkValid() (+32 more)

### Community 2 - "String Task_Tracker"
Cohesion: 0.08
Nodes (13): JobRecord, JobState, ScheduleJobRequest, ScheduleTask, ScheduleTaskInput, SystemConfigUpdate, Task, TaskInputSplit (+5 more)

### Community 3 - "Requirement Workers"
Cohesion: 0.09
Nodes (24): Empty Placeholder File, Authentication Service, CLI-Based UI Requirement, Distributed Data Service, Pod Failure Recovery Requirement, Workers as Kubernetes Jobs, Manager Replication for Resilience, Manager Service (+16 more)

### Community 4 - "Bootstrap Bootstrapconfig"
Cohesion: 0.21
Nodes (9): BootstrapConfig, keycloakBootstrapper, keycloakClient, tokenResponse, BootstrapKeycloak(), BootstrapKeycloakWithContext(), newBootstrapHTTPClient(), newBootstrapTransport() (+1 more)

### Community 5 - "Scheduler Allmaptaskscompleted"
Cohesion: 0.14
Nodes (2): Scheduler, taskMetadataQuerier

### Community 6 - "Roundtripfunc Roundtrip"
Cohesion: 0.12
Nodes (5): roundTripFunc, TestBootstrapKeycloakWithContextCanceled(), TestCallJSONRetriesNetworkErrorsAndReturnsServiceUnavailable(), TestNewKeycloakBootstrapperUsesDefaultHTTPClientTimeouts(), validBootstrapConfig()

### Community 7 - "Handlers Cleanupjobsstore"
Cohesion: 0.18
Nodes (5): Handlers, generateJobID(), isAuthDependencyError(), NewHandlers(), newHandlersWithOptions()

### Community 8 - "Createuserrequest Keycloakadminclient"
Cohesion: 0.25
Nodes (5): CreateUserRequest, KeycloakAdminClient, keycloakTokenResponse, keycloakUser, roleMapping

### Community 9 - "Dds Attemptstatus"
Cohesion: 0.14
Nodes (11): AttemptStatus, Job, JobConfig, JobStatus, SystemConfig, Task, TaskAttempt, TaskInput (+3 more)

### Community 10 - "Apiurl Clirequestcontext"
Cohesion: 0.32
Nodes (11): apiURL(), cliRequestContext(), doAuthRequest(), doAuthRequestExpect(), doAuthRequestWithContext(), getEnv(), getValidToken(), keycloakBaseURL() (+3 more)

### Community 11 - "Clijobfuncspec Clijobpayload"
Cohesion: 0.27
Nodes (9): cliJobFuncSpec, cliJobPayload, cmdJobsDownload(), cmdJobsStatus(), cmdJobsSubmit(), inferLanguage(), jobRequestPath(), safeJobResultFilename() (+1 more)

### Community 12 - "Testexit Error"
Cohesion: 0.2
Nodes (2): testExit, TestValidateReducersCount()

### Community 13 - "Stubkeyfunc Keyfunc"
Cohesion: 0.24
Nodes (6): stubKeyfunc, newTestValidator(), splitAuthHeader(), TestMiddleware_InvalidAuthHeaderFormat(), TestMiddleware_InvalidToken_ReturnsGenericError(), TestMiddleware_MissingAuthHeader()

### Community 14 - "Testrefreshtokens_Connectionerror Testrefreshtokens_Httperror"
Cohesion: 0.18
Nodes (0): 

### Community 15 - "Roles_Test Testcontainsrole"
Cohesion: 0.18
Nodes (0): 

### Community 16 - "Http_Test Testclihttpclient_Hastimeout"
Cohesion: 0.2
Nodes (0): 

### Community 17 - "Task_Tracker_Test Testjobrecord_Fields"
Cohesion: 0.2
Nodes (0): 

### Community 18 - "Validation Badrequesterror"
Cohesion: 0.31
Nodes (6): BadRequestError, NewBadRequestError(), NormalizeRole(), ValidateCreateUserRequest(), validateFunctionSpec(), ValidateJobSubmission()

### Community 19 - "Whoami Cmdtokeninspect"
Cohesion: 0.42
Nodes (8): cmdTokenInspect(), cmdWhoAmI(), decodeTokenClaims(), extractRealmRoles(), hasAdminRole(), hasRealmRole(), loadTokensAndClaims(), runWhoAmI()

### Community 20 - "Testjob_Isterminal Testjobconfig_Serialization"
Cohesion: 0.22
Nodes (0): 

### Community 21 - "Requests Createuserrequest"
Cohesion: 0.22
Nodes (8): CreateUserRequest, DeleteUserRequest, FunctionSpec, JobStatusResponse, JobSubmissionRequest, JobSubmissionResponse, NodeConfigRequest, WorkerConfigRequest

### Community 22 - "Requests_Test Testcreateuserrequest_Json"
Cohesion: 0.22
Nodes (0): 

### Community 23 - "Failingwriteresponsewriter Header"
Cohesion: 0.25
Nodes (2): failingWriteResponseWriter, TestWriteJSON_Success()

### Community 24 - "Cmdadminconfigurenodes Cmdadmincreateuser"
Cohesion: 0.43
Nodes (7): cmdAdminConfigureNodes(), cmdAdminCreateUser(), cmdAdminDeleteUser(), cmdAdminWorkerConfig(), configureNodesStatusError(), requireAdminRole(), runAdminConfigureNodes()

### Community 25 - "Testload_Customenvvars Testload_Defaults"
Cohesion: 0.25
Nodes (0): 

### Community 26 - "Testcliadminroutes_Matchapiroutes Testcmdadminconfigurenodes_Accepts202Andprintsbody"
Cohesion: 0.29
Nodes (0): 

### Community 27 - "Whoami_Test Buildjwtfortest"
Cohesion: 0.33
Nodes (2): buildJWTForTest(), TestCmdWhoAmI_PrintsIdentityForValidToken()

### Community 28 - "Ensurecallstatus Ensurestatus"
Cohesion: 0.29
Nodes (0): 

### Community 29 - "Testassignrealmrolenormalizesandescapesrolelookuppath Testassignrealmrolerejectsemptyrolenameaftertrim"
Cohesion: 0.29
Nodes (0): 

### Community 30 - "Getenv Printusage"
Cohesion: 0.47
Nodes (3): getEnv(), main(), printUsage()

### Community 31 - "Run Defaultsetupdependencies"
Cohesion: 0.33
Nodes (3): adminUserCreator, setupDependencies, setupParams

### Community 32 - "Run_Test Testrunsetup_Bootstrapphasetimeoutstopsflow"
Cohesion: 0.33
Nodes (1): stubAdminClient

### Community 33 - "Validation_Test Testbadrequesthelpers"
Cohesion: 0.4
Nodes (2): TestValidateJobSubmission(), validJobSubmissionRequest()

### Community 34 - "Serviceunavailableerror Error"
Cohesion: 0.33
Nodes (1): ServiceUnavailableError

### Community 35 - "Oauthtokenresponse Refreshtokens"
Cohesion: 0.47
Nodes (5): OAuthTokenResponse, RefreshTokens(), RefreshTokensWithContext(), RequestTokens(), RequestTokensWithContext()

### Community 36 - "Roles Containsrole"
Cohesion: 0.6
Nodes (5): containsRole(), GetRoles(), RequireAnyRole(), RequireRole(), requireRoles()

### Community 37 - "Contextkey Jwtvalidator"
Cohesion: 0.4
Nodes (2): contextKey, JWTValidator

### Community 38 - "Login Cmdhealth"
Cohesion: 0.5
Nodes (0): 

### Community 39 - "Config Getenv"
Cohesion: 0.67
Nodes (3): Config, getEnv(), Load()

### Community 40 - "Response Writeerror"
Cohesion: 1.0
Nodes (2): WriteError(), WriteJSON()

### Community 41 - "Routes Registerroutes"
Cohesion: 1.0
Nodes (0): 

### Community 42 - "Queries"
Cohesion: 1.0
Nodes (0): 

### Community 43 - "In-Memory Job"
Cohesion: 1.0
Nodes (1): In-Memory Job Store Limitation

## Ambiguous Edges - Review These
- `KubeMapReduce Platform` → `Empty Placeholder File`  [AMBIGUOUS]
  foo.txt · relation: conceptually_related_to

## Knowledge Gaps
- **52 isolated node(s):** `cliJobFuncSpec`, `cliJobPayload`, `setupParams`, `adminUserCreator`, `setupDependencies` (+47 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Routes Registerroutes`** (2 nodes): `routes.go`, `RegisterRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Queries`** (1 nodes): `queries.go`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `In-Memory Job`** (1 nodes): `In-Memory Job Store Limitation`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `KubeMapReduce Platform` and `Empty Placeholder File`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What connects `cliJobFuncSpec`, `cliJobPayload`, `setupParams` to the rest of the system?**
  _52 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Fakekeycloak Newtesthandlers` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `Scheduler_Test Expectleasevalidation` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._
- **Should `String Task_Tracker` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `Requirement Workers` be split into smaller, more focused modules?**
  _Cohesion score 0.09 - nodes in this community are weakly interconnected._
- **Should `Scheduler Allmaptaskscompleted` be split into smaller, more focused modules?**
  _Cohesion score 0.14 - nodes in this community are weakly interconnected._
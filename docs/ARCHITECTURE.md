# Architecture Documentation

## Overview

KubeMapReduce is a three-layer distributed MapReduce platform:

- **CLI Service** handles user authentication, token refresh, and job submission.
- **Manager Service** coordinates scheduling, task assignment, leases, heartbeats, and fault recovery.
- **Worker Pods** execute user code, process JSONL input/output, and write results to shared storage.

The platform uses PostgreSQL for task state, MinIO for shared object storage, and gRPC for Manager-to-Worker coordination.

## Core Flows

1. The CLI submits a job to the UI service with a Bearer JWT.
2. The UI validates the token and forwards the job to the Manager.
3. The Manager schedules work, assigns a lease, and issues a task attempt.
4. Workers renew their lease with heartbeats and report completion or failure.
5. The Manager commits only the current attempt and rejects stale results.

## Supporting References

- [Event-Driven Coordination Proposal](ARCHITECTURE_EVENT_DRIVEN_COORDINATION.md) (Draft)
- [Data Locality-Aware Scheduling](ARCHITECTURE_DATA_LOCALITY.md) (Implemented)
- [External Access Architecture](ARCHITECTURE_EXTERNAL_ACCESS.md)
- [Deployment Guide](DEPLOYMENT.md)
- [External Access Guide](EXTERNAL_ACCESS.md)
- [gRPC API Reference](api/grpc.md)
- [REST API Reference](api/rest.md)

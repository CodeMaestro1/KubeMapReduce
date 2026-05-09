# Architecture: Data Locality-Aware Scheduling

## Goal
Optimize MapReduce job performance by reducing cross-node and cross-AZ network traffic. By scheduling worker pods on the same topology domain (node, rack, or zone) as the input and shuffle data, we minimize latency and cluster bandwidth consumption.

## Mechanism: Kubernetes Pod Affinity
The platform uses **Kubernetes Soft Pod Affinity** to express locality preferences.

When the Manager orchestrates a worker pool for a job, it injects a `preferredDuringSchedulingIgnoredDuringExecution` rule into the worker Deployment:

- **Label Selector**: `app.kubernetes.io/name: minio`
- **Topology Key**: Configurable (defaults to `topology.kubernetes.io/zone`)
- **Weight**: 100

### Why Soft Constraints?
We use "Preferred" (soft) constraints rather than "Required" (hard) to ensure **High Availability**:
1. If a node or zone holding data is at capacity, the worker will be scheduled elsewhere rather than remaining pending forever.
2. The existing fault-tolerance model (lease fencing and task reapers) continues to function regardless of where the pod lands.

## Configuration
Administrators can tune the locality behavior via the CLI:

```bash
# co-locate workers in the same zone as MinIO (default)
kubemapreduce admin configure-nodes --locality-key topology.kubernetes.io/zone

# co-locate workers on the same physical node as MinIO
kubemapreduce admin configure-nodes --locality-key kubernetes.io/hostname

# use a custom label selector for target pods (e.g. external MinIO with different labels)
kubemapreduce admin configure-nodes --locality-label-selector app.kubernetes.io/name=minio-external

# disable locality scheduling
kubemapreduce admin configure-nodes --locality-key ""
```

## Data Lifecycle Locality
1. **Map Phase**: Workers pull input splits from MinIO. Co-location reduces the initial download time.
2. **Shuffle Phase**: Map outputs are written back to MinIO (or intermediate storage). Reducers then pull these partitions. Co-location here minimizes the network cost of the shuffle "all-to-all" transfer.

## Current Limitation: Storage-Tier Locality vs. Per-Split Data Locality

The current implementation provides **storage-tier locality** — it biases the entire worker
Deployment toward nodes/zones that already host MinIO pods. This is not the same as
Hadoop-style **per-split data locality**, where each individual task is placed on the node
that physically holds its input bytes.

Specifically:
- All workers for a job receive the **same** `PodAffinity` rule (it is on the Deployment).
- Two Map workers processing different input splits will both prefer the MinIO zone, not
  their own respective input node.
- The Manager has no per-object placement metadata from MinIO (MinIO erasure-codes objects
  across drives and pools, and does not expose per-object node placement via a public API).

This still meaningfully reduces cross-AZ egress when MinIO is zone-pinned, and satisfies
the acceptance criterion of expressing locality preferences as soft constraints in
placement decisions.

### Path to per-split data locality (future work)
True per-task locality would require:
1. Querying MinIO admin/placement APIs to learn which nodes hold each input object's primary shard.
2. Emitting per-pod `NodeAffinity` (or switching to Job-per-task) so each worker targets its own input node.
3. Graceful fallback when placement metadata is unavailable (single-node MinIO, gateway mode, S3).

## Required Signals
The scheduler relies on standard Kubernetes node labels:
- `topology.kubernetes.io/zone` (Automatic on cloud providers like Okeanos)
- `kubernetes.io/hostname` (Standard on all clusters)

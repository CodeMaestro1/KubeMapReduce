# Data Locality Scheduling

This document describes what the locality features added in PR #276 actually do,
and — equally important — what they do **not** do.

## What is implemented

The Manager attaches a `PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution`
rule to every worker pool `Deployment` it creates. The rule is built from two
DB-backed `SYSTEM_CONFIG` columns:

| Column                     | Default                          | Meaning                                                                 |
| -------------------------- | -------------------------------- | ----------------------------------------------------------------------- |
| `locality_key`             | `topology.kubernetes.io/zone`    | The Kubernetes topology key the scheduler should match on.              |
| `locality_label_selector`  | `app=minio`                      | A `key=value[,key=value]` selector identifying the *target* pods.       |

In plain terms: *prefer scheduling worker pods onto nodes (or zones) that already
run a pod matching the selector* — by default, the MinIO storage pods.

This is **storage-tier locality**: it pulls compute toward the nodes that host
the storage service. When MinIO is rack-aware or zone-pinned, this can
materially reduce cross-zone traffic and egress costs.

## What is NOT implemented

This is **not** Hadoop/MapReduce-style per-split data locality. Specifically:

1. The Manager has no knowledge of which physical node holds the bytes of a
   given input split. MinIO erasure-codes objects across drives and pools, and
   the Manager never queries the resulting placement.
2. The affinity rule lives on the worker `Deployment`, not on individual pods,
   so two workers reading two different splits still receive identical affinity
   terms.
3. The rule is `Preferred…`, not `Required…`. The scheduler is free to place
   the pod anywhere if the preferred zone is full.

In other words: workers are biased toward *the storage tier*, not toward *their
own input*.

## Path to true per-task data locality

A future change would need to:

1. Have the Manager call MinIO's admin / placement API (or precompute placement
   from the bucket layout) to learn which nodes physically hold each
   `input_uri` byte range.
2. Stop relying on a single Deployment-wide affinity rule. Instead, render
   per-task `NodeAffinity` (or use a Job-per-task model) so each worker pod
   gets a hard preference for the node hosting its split.
3. Fall back gracefully when placement metadata is unavailable (single-node
   MinIO, gateway mode, S3, etc.).

This is intentionally deferred: it requires a sizeable change to the worker
spawning model (Deployment → Job-per-task) and a new dependency on MinIO
admin APIs.

## Configuration

Operators tune the feature through the admin API:

```http
POST /api/v1/admin/config/workers
{
  "localityKey": "kubernetes.io/hostname",
  "localityLabelSelector": "app=minio,tier=storage"
}
```

Setting either field to an empty string disables the affinity term entirely.

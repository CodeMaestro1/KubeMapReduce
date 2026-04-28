# Kubernetes Manifests

Declarative deployment for the KubeMapReduce platform. All workloads run in
the `mapreduce` namespace.

## Layout

| File                       | Resources                                                |
|----------------------------|----------------------------------------------------------|
| `00-namespace.yaml`        | `mapreduce` Namespace                                    |
| `10-postgres.yaml`         | PostgreSQL StatefulSet (RWO PVC) + Secret + init schema  |
| `20-minio.yaml`            | MinIO StatefulSet (RWX PVC) + `minio-creds` Secret       |
| `25-keycloak.yaml`         | Keycloak Deployment + admin Secret                       |
| `30-manager.yaml`          | Manager StatefulSet (3 replicas) + `manager-headless`    |
| `40-ui.yaml`               | UI Deployment (stateless, 2 replicas)                    |
| `50-worker-rbac.yaml`      | `manager` SA + Role/Binding + unprivileged `worker` SA   |
| `60-gateway.yaml`          | Gateway + HTTPRoutes (api/storage/auth.mapreduce.local)  |
| `kustomization.yaml`       | Apply order for `kubectl apply -k`                       |

## Deploy

```bash
# 1. Bake the migration into the postgres-init ConfigMap.
kubectl -n mapreduce create configmap postgres-init \
  --from-file=migrations/ --dry-run=client -o yaml | kubectl apply -f -

# 2. Apply everything.
kubectl apply -k k8s/

# 3. Wait for rollouts.
kubectl -n mapreduce rollout status statefulset/postgres
kubectl -n mapreduce rollout status statefulset/minio
kubectl -n mapreduce rollout status deployment/keycloak
kubectl -n mapreduce rollout status statefulset/manager
kubectl -n mapreduce rollout status deployment/ui
```

## Secrets

No credentials are inlined in any workload manifest. Every service reads its
secrets via `secretKeyRef`:

| Secret             | Consumers                       |
|--------------------|---------------------------------|
| `postgres-creds`   | postgres, manager, ui           |
| `minio-creds`      | minio, manager, ui              |
| `keycloak-creds`   | keycloak                        |
| `manager-secrets`  | manager, ui                     |
| `mapreduce-tls`    | gateway (TLS termination)       |

Override the placeholder values before applying to anything resembling a
production cluster:

```bash
kubectl -n mapreduce create secret generic minio-creds \
  --from-literal=MINIO_ROOT_USER=mapreduce \
  --from-literal=MINIO_ROOT_PASSWORD="$(openssl rand -hex 32)" \
  --from-literal=S3_ACCESS_KEY=mapreduce \
  --from-literal=S3_SECRET_KEY="$(openssl rand -hex 32)" \
  --from-literal=S3_ENDPOINT=minio.mapreduce.svc.cluster.local:9000 \
  --dry-run=client -o yaml | kubectl apply -f -
```

## External access

| Hostname                   | Backend Service          |
|----------------------------|--------------------------|
| `api.mapreduce.local`      | `ui:8080`                |
| `storage.mapreduce.local`  | `minio:9000`             |
| `auth.mapreduce.local`     | `keycloak:8080`          |

Add these names to your local `/etc/hosts` pointing at the Gateway's external
IP, or configure DNS for whatever domain you own. TLS terminates at the
Gateway using the `mapreduce-tls` Secret.

## Probes

| Workload   | Liveness            | Readiness           |
|------------|---------------------|---------------------|
| postgres   | `pg_isready` exec   | `pg_isready` exec   |
| minio      | `/minio/health/live`| `/minio/health/ready`|
| keycloak   | `/realms/master`    | `/realms/master`    |
| manager    | `/health` (8081)    | `/readyz` (8081)    |
| ui         | `/health` (8080)    | `/health` (8080)    |

## Worker Jobs

Workers are not deployed via static manifests. The Manager creates `batch/v1`
Job objects at runtime using the `manager` ServiceAccount. Worker pods run as
the unprivileged `worker` ServiceAccount with `automountServiceAccountToken:
false` so they cannot reach the Kubernetes API. This is the contract enforced
by `50-worker-rbac.yaml`.

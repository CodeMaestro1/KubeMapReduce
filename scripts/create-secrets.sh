#!/usr/bin/env bash
# Creates all Kubernetes secrets required by KubeMapReduce.
# Run once before the first deploy, or after wiping the namespace.
# Safe to re-run — uses --dry-run=client | kubectl apply to avoid conflicts.
#
# Usage:
#   bash scripts/create-secrets.sh
#   NODE_IP=<ip> bash scripts/create-secrets.sh   # use external MinIO endpoint
#
# NODE_IP controls the S3_ENDPOINT in minio-creds. MinIO presigned URLs are
# generated with this address, so the CLI must be able to reach it.
# Defaults to the internal cluster DNS (only works if CLI runs inside cluster).
set -euo pipefail

NAMESPACE="${NAMESPACE:-mapreduce}"
NODE_IP="${NODE_IP:-}"

if [[ -z "$NODE_IP" ]]; then
  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)
fi

S3_ENDPOINT="${NODE_IP:+$NODE_IP:30900}"
S3_ENDPOINT="${S3_ENDPOINT:-minio.mapreduce.svc.cluster.local:9000}"

echo "Creating secrets in namespace: $NAMESPACE"
echo "MinIO S3 endpoint: $S3_ENDPOINT"

kubectl -n "$NAMESPACE" create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=mapreduce \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=POSTGRES_DB=mapreduce \
  --dry-run=client -o yaml | kubectl apply -f -

MINIO_PASSWORD="$(openssl rand -hex 32)"
kubectl -n "$NAMESPACE" create secret generic minio-creds \
  --from-literal=MINIO_ROOT_USER=mapreduce \
  --from-literal=MINIO_ROOT_PASSWORD="$MINIO_PASSWORD" \
  --from-literal=S3_ACCESS_KEY=mapreduce \
  --from-literal=S3_SECRET_KEY="$MINIO_PASSWORD" \
  --from-literal=S3_ENDPOINT="$S3_ENDPOINT" \
  --dry-run=client -o yaml | kubectl apply -f -

WORKER_RPC_TOKEN="$(openssl rand -hex 16)"
kubectl -n "$NAMESPACE" create secret generic manager-secrets \
  --from-literal=MANAGER_INTERNAL_API_KEY="$(openssl rand -hex 16)" \
  --from-literal=MANAGER_WORKER_RPC_TOKEN="$WORKER_RPC_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

# kubemapreduce-secrets is mounted into every worker pod by KubeOrchestrator.
# Keys must match what orchestrator.go reads: MINIO_ENDPOINT, MINIO_ACCESS_KEY,
# MINIO_SECRET_KEY, MINIO_BUCKET, MANAGER_WORKER_RPC_TOKEN.
kubectl -n "$NAMESPACE" create secret generic kubemapreduce-secrets \
  --from-literal=MINIO_ENDPOINT="$S3_ENDPOINT" \
  --from-literal=MINIO_ACCESS_KEY=mapreduce \
  --from-literal=MINIO_SECRET_KEY="$MINIO_PASSWORD" \
  --from-literal=MINIO_BUCKET=mapreduce-shuffle \
  --from-literal=MANAGER_WORKER_RPC_TOKEN="$WORKER_RPC_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" create secret generic keycloak-creds \
  --from-literal=KEYCLOAK_ADMIN=admin \
  --from-literal=KEYCLOAK_ADMIN_PASSWORD="$(openssl rand -hex 16)" \
  --dry-run=client -o yaml | kubectl apply -f -

# TLS cert for gRPC (self-signed)
if ! kubectl -n "$NAMESPACE" get secret grpc-tls &>/dev/null; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout /tmp/grpc-tls.key -out /tmp/grpc-tls.crt \
    -subj "/CN=manager" \
    -addext "subjectAltName=DNS:*.manager-headless.${NAMESPACE}.svc.cluster.local"
  kubectl -n "$NAMESPACE" create secret tls grpc-tls \
    --cert=/tmp/grpc-tls.crt --key=/tmp/grpc-tls.key
  rm /tmp/grpc-tls.key /tmp/grpc-tls.crt
fi

echo "Done. Restart pods to pick up new secrets:"
echo "  kubectl -n $NAMESPACE rollout restart statefulset/postgres statefulset/minio statefulset/manager"

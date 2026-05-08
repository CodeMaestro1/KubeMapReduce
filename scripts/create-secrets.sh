#!/usr/bin/env bash
# Creates all Kubernetes secrets required by KubeMapReduce.
# Run once before the first deploy, or after wiping the namespace.
# Safe to re-run — uses --dry-run=client | kubectl apply to avoid conflicts.
set -euo pipefail

NAMESPACE="${NAMESPACE:-mapreduce}"

echo "Creating secrets in namespace: $NAMESPACE"

kubectl -n "$NAMESPACE" create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=mapreduce \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=POSTGRES_DB=mapreduce \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" create secret generic minio-creds \
  --from-literal=MINIO_ROOT_USER=mapreduce \
  --from-literal=MINIO_ROOT_PASSWORD="$(openssl rand -hex 32)" \
  --from-literal=S3_ACCESS_KEY=mapreduce \
  --from-literal=S3_SECRET_KEY="$(openssl rand -hex 32)" \
  --from-literal=S3_ENDPOINT=minio.mapreduce.svc.cluster.local:9000 \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" create secret generic manager-secrets \
  --from-literal=MANAGER_INTERNAL_API_KEY="$(openssl rand -hex 16)" \
  --from-literal=MANAGER_WORKER_RPC_TOKEN="$(openssl rand -hex 16)" \
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

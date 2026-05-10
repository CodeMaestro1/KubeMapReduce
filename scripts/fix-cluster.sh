#!/usr/bin/env bash
# Fix recurring cluster issues after kubectl apply -k:
#   1. Service selectors polluted by kustomize commonLabels (part-of label)
#   2. NodePorts not exposed (keycloak:30080, api:30081, minio:30900/30901)
#
# Run after every `kubectl apply -k k8s/` until kustomize overlays are set up.
#
# Usage: bash scripts/fix-cluster.sh
set -euo pipefail

NS="${NAMESPACE:-mapreduce}"

echo "==> Fixing service selectors (removing part-of label)..."

for svc in postgres minio manager manager-headless keycloak api; do
  if kubectl -n "$NS" get svc "$svc" &>/dev/null; then
    SELECTOR=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.selector}')
    if echo "$SELECTOR" | grep -q "part-of"; then
      NAME=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.selector.app\.kubernetes\.io/name}')
      kubectl -n "$NS" patch svc "$svc" --type=json \
        -p="[{\"op\":\"replace\",\"path\":\"/spec/selector\",\"value\":{\"app.kubernetes.io/name\":\"$NAME\"}}]"
      echo "  patched svc/$svc selector"
    fi
  fi
done

echo "==> Exposing NodePorts..."

# Keycloak: 30080
KC_TYPE=$(kubectl -n "$NS" get svc keycloak -o jsonpath='{.spec.type}')
if [[ "$KC_TYPE" != "NodePort" ]]; then
  kubectl -n "$NS" patch svc keycloak --type=json \
    -p='[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"add","path":"/spec/ports/0/nodePort","value":30080}]'
  echo "  keycloak -> NodePort 30080"
fi

# API: 30081
API_TYPE=$(kubectl -n "$NS" get svc api -o jsonpath='{.spec.type}')
if [[ "$API_TYPE" != "NodePort" ]]; then
  kubectl -n "$NS" patch svc api --type=json \
    -p='[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"add","path":"/spec/ports/0/nodePort","value":30081}]'
  echo "  api -> NodePort 30081"
fi

# MinIO: 30900 (S3), 30901 (console)
MINIO_TYPE=$(kubectl -n "$NS" get svc minio -o jsonpath='{.spec.type}')
if [[ "$MINIO_TYPE" != "NodePort" ]]; then
  kubectl -n "$NS" patch svc minio --type=json \
    -p='[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"add","path":"/spec/ports/0/nodePort","value":30900},{"op":"add","path":"/spec/ports/1/nodePort","value":30901}]'
  echo "  minio -> NodePort 30900 (S3), 30901 (console)"
fi

echo "==> Creating MinIO buckets..."
MINIO_USER=$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_PASS=$(kubectl -n "$NS" get secret minio-creds -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
kubectl -n "$NS" exec minio-0 -- sh -c "
  mc alias set local http://localhost:9000 '$MINIO_USER' '$MINIO_PASS' &&
  mc mb --ignore-existing local/mapreduce-inputs &&
  mc mb --ignore-existing local/mapreduce-outputs &&
  mc mb --ignore-existing local/mapreduce-shuffle
" && echo "  buckets ready" || echo "  warning: bucket creation failed (minio may not be ready yet)"

echo "==> Done. Endpoints:"
kubectl -n "$NS" get svc keycloak api minio --no-headers | awk '{print "  "$1": "$5}'

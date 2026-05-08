#!/usr/bin/env bash
# Applies all SQL migrations to postgres in order.
# Safe to re-run if migrations use IF NOT EXISTS / CREATE TABLE IF NOT EXISTS.
set -euo pipefail

NAMESPACE="${NAMESPACE:-mapreduce}"
POD="${POD:-postgres-0}"

echo "Waiting for postgres pod $POD to be ready..."
kubectl wait pod "$POD" -n "$NAMESPACE" --for=condition=Ready --timeout=60s

for f in $(ls migrations/*.sql | sort); do
  echo "Applying $f..."
  kubectl exec -i "$POD" -n "$NAMESPACE" -- psql -U mapreduce -d mapreduce < "$f"
done

echo "All migrations applied."

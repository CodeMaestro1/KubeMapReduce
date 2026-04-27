#!/usr/bin/env bash
# serve-docs.sh — Spin up the full KubeMapReduce documentation stack locally.
#
# Usage:
#   ./serve-docs.sh          — start everything
#   ./serve-docs.sh --stop   — kill background processes

set -euo pipefail

PKGSITE_PORT=8080
MKDOCS_PORT=8000

stop() {
  echo "Stopping doc servers..."
  pkill -f "pkgsite" 2>/dev/null || true
  pkill -f "mkdocs serve" 2>/dev/null || true
  echo "Done."
  exit 0
}

[[ "${1:-}" == "--stop" ]] && stop

# ── Dependency check ────────────────────────────────────────────────────────

check() {
  if ! command -v "$1" &>/dev/null; then
    echo "Missing: $1. Install with: $2"
    exit 1
  fi
}

check pkgsite   "go install golang.org/x/pkgsite/cmd/pkgsite@latest"
check mkdocs    "pip install mkdocs mkdocs-material pymdown-extensions"

# ── pkgsite (GoDoc → HTML) ───────────────────────────────────────────────────

echo "Starting pkgsite on http://localhost:${PKGSITE_PORT} ..."
pkgsite -http ":${PKGSITE_PORT}" &>/tmp/pkgsite.log &
PKGSITE_PID=$!

# ── MkDocs (Markdown docs → HTML) ───────────────────────────────────────────

echo "Starting MkDocs on http://localhost:${MKDOCS_PORT} ..."
mkdocs serve -a "localhost:${MKDOCS_PORT}" &>/tmp/mkdocs.log &
MKDOCS_PID=$!

# ── Summary ──────────────────────────────────────────────────────────────────

sleep 1  # let servers bind

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║           KubeMapReduce Docs — Running               ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Go API reference  →  http://localhost:${PKGSITE_PORT}        ║"
echo "║  Architecture docs →  http://localhost:${MKDOCS_PORT}        ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Logs:  /tmp/pkgsite.log  /tmp/mkdocs.log            ║"
echo "║  Stop:  ./serve-docs.sh --stop                       ║"
echo "╚══════════════════════════════════════════════════════╝"

# Keep script alive so Ctrl+C stops both servers cleanly
trap 'kill $PKGSITE_PID $MKDOCS_PID 2>/dev/null; echo "Stopped."' INT TERM
wait
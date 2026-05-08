#!/usr/bin/env bash
# serve-docs.sh — Spin up the full KubeMapReduce documentation stack locally.
#
# Usage:
#   ./serve-docs.sh          — start everything
#   ./serve-docs.sh --stop   — kill background processes

set -euo pipefail

REDIRECT_PORT=8082
MKDOCS_PORT=8000
REDIRECT_PID_FILE=/tmp/kubemapreduce-docs-redirect.pid
MKDOCS_PID_FILE=/tmp/kubemapreduce-mkdocs.pid

stop() {
  echo "Stopping doc servers..."
  if [[ -f "$REDIRECT_PID_FILE" ]]; then
    kill "$(cat "$REDIRECT_PID_FILE")" 2>/dev/null || true
    rm -f "$REDIRECT_PID_FILE"
  fi
  if [[ -f "$MKDOCS_PID_FILE" ]]; then
    kill "$(cat "$MKDOCS_PID_FILE")" 2>/dev/null || true
    rm -f "$MKDOCS_PID_FILE"
  fi
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

PYTHON_CMD="$(command -v python3 2>/dev/null || command -v python 2>/dev/null || true)"
if [[ -z "$PYTHON_CMD" ]]; then
  echo "Missing: python3. Install Python 3."
  exit 1
fi
if ! "$PYTHON_CMD" -c 'import mkdocs' >/dev/null 2>&1; then
  echo "Missing: mkdocs. Install with: pip install mkdocs mkdocs-material pymdown-extensions"
  exit 1
fi

# ── docs redirect (8082 → MkDocs paths) ─────────────────────────────────────

echo "Starting docs redirect on http://localhost:${REDIRECT_PORT} ..."
"$PYTHON_CMD" - <<'PY' &>/tmp/pkgsite.log &
import http.server
import socketserver

PORT = 8082
TARGET = "http://localhost:8000"


class RedirectHandler(http.server.BaseHTTPRequestHandler):
  def do_GET(self):
    path = self.path.split("?", 1)[0]
    if path == "/kubemapreduce":
      path = "/"
    elif path.startswith("/kubemapreduce/"):
      path = path[len("/kubemapreduce"):]

    if not path.endswith("/") and "." not in path.rsplit("/", 1)[-1]:
      path = f"{path}/"

    self.send_response(302)
    self.send_header("Location", f"{TARGET}{path}")
    self.end_headers()

  def log_message(self, format, *args):
    return


with socketserver.TCPServer(("127.0.0.1", PORT), RedirectHandler) as server:
  server.serve_forever()
PY
PKGSITE_PID=$!
printf '%s' "$PKGSITE_PID" > "$REDIRECT_PID_FILE"

# ── MkDocs (Markdown docs → HTML) ───────────────────────────────────────────

echo "Starting MkDocs on http://localhost:${MKDOCS_PORT} ..."
"$PYTHON_CMD" -m mkdocs serve -a "localhost:${MKDOCS_PORT}" &>/tmp/mkdocs.log &
MKDOCS_PID=$!
printf '%s' "$MKDOCS_PID" > "$MKDOCS_PID_FILE"

# ── Summary ──────────────────────────────────────────────────────────────────

sleep 1  # let servers bind

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║           KubeMapReduce Docs — Running               ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Docs redirect     →  http://localhost:${REDIRECT_PORT}        ║"
echo "║  Architecture docs →  http://localhost:${MKDOCS_PORT}        ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Logs:  /tmp/pkgsite.log  /tmp/mkdocs.log            ║"
echo "║  Stop:  ./serve-docs.sh --stop                       ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Use http://localhost:${REDIRECT_PORT}/kubemapreduce/auth-service/pkg/auth"
echo "to reach the MkDocs package page at http://localhost:${MKDOCS_PORT}/auth-service/pkg/auth/"

# Keep script alive so Ctrl+C stops both servers cleanly
trap 'kill $PKGSITE_PID $MKDOCS_PID 2>/dev/null; echo "Stopped."' INT TERM
wait
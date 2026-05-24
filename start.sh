#!/bin/bash
# ─── OSINT Pipeline — Start Everything ────────────────────────────────────────
# Starts Docker services, FastAPI, and ARQ worker in one terminal.
# Ctrl+C stops everything cleanly.
#
# Usage:
#   ./start.sh          — start on default port 8080
#   ./start.sh 9000     — start on custom port
# ──────────────────────────────────────────────────────────────────────────────

set -e
PORT=${1:-8080}
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/.venv/bin"

# ── Colors ─────────────────────────────────────────────────────────────────────
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
DIM='\033[2m'
NC='\033[0m'

log() { echo -e "${DIM}[start]${NC} $1"; }
ok()  { echo -e "${GREEN}✓${NC} $1"; }
err() { echo -e "${RED}✗${NC} $1"; }

# ── Cleanup on exit ────────────────────────────────────────────────────────────
cleanup() {
  echo ""
  log "Shutting down..."
  kill "$API_PID" "$WORKER_PID" 2>/dev/null
  wait "$API_PID" "$WORKER_PID" 2>/dev/null
  log "Done."
}
trap cleanup EXIT INT TERM

# ── 1. Docker services ─────────────────────────────────────────────────────────
log "Checking Docker services..."
cd "$PROJECT_DIR"
docker compose up -d --quiet-pull 2>/dev/null || {
  err "Docker Compose failed. Is Docker Desktop running?"
  exit 1
}

# Wait for Redis to be healthy (required before worker starts)
for i in $(seq 1 15); do
  if docker exec osint_redis redis-cli ping 2>/dev/null | grep -q PONG; then
    ok "Redis ready"
    break
  fi
  if [ "$i" -eq 15 ]; then
    err "Redis did not become healthy after 15s"
    exit 1
  fi
  sleep 1
done

# ── 2. Clear port ──────────────────────────────────────────────────────────────
if lsof -ti:$PORT &>/dev/null; then
  log "Clearing port $PORT..."
  lsof -ti:$PORT | xargs kill -9 2>/dev/null || true
  sleep 1
fi

# ── 3. Start FastAPI ───────────────────────────────────────────────────────────
log "Starting FastAPI on port $PORT..."
cd "$PROJECT_DIR"
(
  "$VENV/uvicorn" osint.api.app:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    2>&1 | while IFS= read -r line; do
      echo -e "${CYAN}[api]${NC} $line"
    done
) &
API_PID=$!

# Wait for API to be healthy
for i in $(seq 1 20); do
  if curl -sf "http://localhost:$PORT/health" | grep -q '"status":"ok"' 2>/dev/null; then
    ok "API ready at http://localhost:$PORT"
    break
  fi
  if ! kill -0 "$API_PID" 2>/dev/null; then
    err "API process died during startup"
    exit 1
  fi
  if [ "$i" -eq 20 ]; then
    err "API did not become healthy after 20s"
    exit 1
  fi
  sleep 1
done

# ── 4. Start ARQ worker ────────────────────────────────────────────────────────
log "Starting ARQ worker..."
(
  "$VENV/arq" osint.workers.worker.WorkerSettings \
    2>&1 | while IFS= read -r line; do
      echo -e "${YELLOW}[worker]${NC} $line"
    done
) &
WORKER_PID=$!

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  OSINT pipeline running. Ctrl+C to stop.${NC}"
echo -e "${GREEN}  API:    http://localhost:$PORT${NC}"
echo -e "${GREEN}  Trigger: ./run.sh \"City Name\" \"Country\"${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ── 5. Wait for either process to die ─────────────────────────────────────────
# Note: `wait -n` is not available on macOS bash 3.2 — poll instead.
while true; do
  sleep 3
  if ! kill -0 "$API_PID" 2>/dev/null; then
    err "API process exited unexpectedly. Check logs above."
    break
  fi
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    err "Worker process exited unexpectedly. Check logs above."
    break
  fi
done

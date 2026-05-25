#!/bin/bash
# ─── OSINT Pipeline — Single Command Launcher ─────────────────────────────────
#
# Usage:
#   ./go.sh                              — start infra + worker (daemon mode)
#   ./go.sh "Philadelphia" "United States"  — start everything + run + poll
#   ./go.sh "Austin"                     — start everything + run (US default)
#
# One terminal. Ctrl+C kills everything cleanly.
# Worker auto-restarts if it crashes — no manual intervention needed.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

CITY="${1:-}"
COUNTRY="${2:-United States}"
PORT="${PORT:-8080}"
API_KEY="df254048a96e4459b52191b8a07ee528c5cc39dfe91af4a3d58a01944ae0c861"
BASE_URL="http://localhost:$PORT"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/.venv/bin"

# ── Colors ─────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

log_api()    { echo -e "${CYAN}[api]${NC}    $*"; }
log_worker() { echo -e "${YELLOW}[worker]${NC} $*"; }
log_run()    { echo -e "${GREEN}[run]${NC}    $*"; }
log_info()   { echo -e "${DIM}[go]${NC}     $*"; }
log_err()    { echo -e "${RED}[error]${NC}  $*"; }

# ── Process group cleanup ──────────────────────────────────────────────────────
# All child processes share this script's process group.
# Ctrl+C sends SIGINT to the group, cleanup() kills any stragglers.
API_PID=""
WORKER_LOOP_PID=""

cleanup() {
  echo ""
  log_info "Shutting down..."
  # Kill the worker restart loop and its current worker child
  if [[ -n "$WORKER_LOOP_PID" ]]; then
    kill -- -$$ 2>/dev/null || true   # kill entire process group
  fi
  if [[ -n "$API_PID" ]]; then
    kill "$API_PID" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
  log_info "Done."
}
trap cleanup EXIT INT TERM

# ── 1. Docker services ─────────────────────────────────────────────────────────
log_info "Starting Docker services..."
cd "$PROJECT_DIR"
docker compose up -d --quiet-pull 2>/dev/null || {
  log_err "Docker Compose failed. Is Docker Desktop running?"
  exit 1
}

# Wait for Redis
for i in $(seq 1 20); do
  if docker exec osint_redis redis-cli ping 2>/dev/null | grep -q PONG; then
    log_info "Redis ready"
    break
  fi
  [[ "$i" -eq 20 ]] && { log_err "Redis did not become healthy"; exit 1; }
  sleep 1
done

# ── 2. Clear port ──────────────────────────────────────────────────────────────
if lsof -ti:$PORT &>/dev/null; then
  log_info "Clearing port $PORT..."
  lsof -ti:$PORT | xargs kill -9 2>/dev/null || true
  sleep 1
fi

# ── 3. Start FastAPI ───────────────────────────────────────────────────────────
log_info "Starting API on port $PORT..."
(
  "$VENV/uvicorn" osint.api.app:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    2>&1 | while IFS= read -r line; do log_api "$line"; done
) &
API_PID=$!

# Wait for API health
for i in $(seq 1 20); do
  if curl -sf "$BASE_URL/health" -H "X-API-Key: $API_KEY" 2>/dev/null | grep -q '"status":"ok"'; then
    log_info "API ready at $BASE_URL"
    break
  fi
  if ! kill -0 "$API_PID" 2>/dev/null; then
    log_err "API died during startup"
    exit 1
  fi
  [[ "$i" -eq 20 ]] && { log_err "API did not become healthy"; exit 1; }
  sleep 1
done

# ── 4. Start ARQ worker — with auto-restart loop ───────────────────────────────
# If the worker crashes (e.g. uncaught exception, OOM), it restarts automatically
# after a 3-second pause. No manual intervention required.
(
  RESTART_COUNT=0
  while true; do
    if [[ "$RESTART_COUNT" -gt 0 ]]; then
      log_worker "Restarting after crash (attempt $RESTART_COUNT)..."
      sleep 3
    fi
    log_worker "Starting (pid $$)..."
    "$VENV/arq" osint.workers.worker.WorkerSettings \
      2>&1 | while IFS= read -r line; do log_worker "$line"; done
    EXIT_CODE=$?
    log_worker "Exited with code $EXIT_CODE"
    RESTART_COUNT=$((RESTART_COUNT + 1))
    # Safety: if worker has crashed 10 times in rapid succession, bail
    if [[ "$RESTART_COUNT" -ge 10 ]]; then
      log_err "Worker crashed $RESTART_COUNT times — giving up. Check logs above."
      exit 1
    fi
  done
) &
WORKER_LOOP_PID=$!

# Give worker a moment to connect and register
sleep 3
log_info "Worker running with auto-restart"

# ── 5. If no city given, run in daemon mode ────────────────────────────────────
if [[ -z "$CITY" ]]; then
  echo ""
  echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${GREEN}  OSINT pipeline running (daemon mode)${NC}"
  echo -e "${GREEN}  API:    $BASE_URL${NC}"
  echo -e "${GREEN}  Trigger: ./go.sh \"City\" \"Country\"${NC}"
  echo -e "${GREEN}  Stop:    Ctrl+C${NC}"
  echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  # Wait for either process to die
  while true; do
    sleep 5
    if ! kill -0 "$API_PID" 2>/dev/null; then
      log_err "API exited. Check output above."
      break
    fi
    if ! kill -0 "$WORKER_LOOP_PID" 2>/dev/null; then
      log_err "Worker loop exited. Check output above."
      break
    fi
  done
  exit 0
fi

# ── 6. Trigger run ─────────────────────────────────────────────────────────────
echo ""
log_run "Triggering run: ${BOLD}$CITY, $COUNTRY${NC}"

RESPONSE=$(curl -sf -X POST "$BASE_URL/runs" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d "{\"city_name\": \"$CITY\", \"country_or_region\": \"$COUNTRY\"}" 2>&1)

if [[ $? -ne 0 ]]; then
  log_err "Failed to reach API at $BASE_URL — is it healthy?"
  exit 1
fi

RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['run_id'])" 2>/dev/null)
if [[ -z "$RUN_ID" ]]; then
  log_err "API error: $RESPONSE"
  exit 1
fi

log_run "Queued — run_id: ${DIM}$RUN_ID${NC}"
echo ""

# ── 7. Poll for completion ─────────────────────────────────────────────────────
POLL_INTERVAL=15
ELAPSED=0
LAST_STATUS=""

while true; do
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))

  STATUS_JSON=$(curl -sf "$BASE_URL/runs/$RUN_ID" \
    -H "X-API-Key: $API_KEY" 2>/dev/null) || continue

  STATUS=$(echo "$STATUS_JSON" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null)
  ENTITIES=$(echo "$STATUS_JSON" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('total_entities_found',0))" 2>/dev/null)
  RELS=$(echo "$STATUS_JSON" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('total_relationships_found',0))" 2>/dev/null)

  case "$STATUS" in
    pending)
      [[ "$STATUS" != "$LAST_STATUS" ]] && log_run "Waiting for worker..."
      ;;
    running)
      log_run "${ELAPSED}s — entities: $ENTITIES | relationships: $RELS"
      ;;
    complete)
      DURATION=$(echo "$STATUS_JSON" | python3 -c \
        "import sys,json; print(json.load(sys.stdin).get('duration_seconds','?'))" 2>/dev/null)
      echo ""
      echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
      echo -e "${GREEN}  ✓ Complete in ${DURATION}s${NC}"
      echo -e "${GREEN}  Entities:      $ENTITIES${NC}"
      echo -e "${GREEN}  Relationships: $RELS${NC}"
      echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
      echo ""
      echo -e "${DIM}Fetch briefing:${NC}"
      echo -e "${DIM}  curl -s $BASE_URL/runs/$RUN_ID/briefing -H 'X-API-Key: $API_KEY' | python3 -m json.tool${NC}"
      # Keep infra alive after run completes so you can query it
      log_info "Infra still running. Ctrl+C to stop."
      wait
      exit 0
      ;;
    failed)
      REASON=$(echo "$STATUS_JSON" | python3 -c \
        "import sys,json; print(json.load(sys.stdin).get('failure_reason','unknown'))" 2>/dev/null)
      echo ""
      echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
      echo -e "${RED}  ✗ Run FAILED: $REASON${NC}"
      echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
      log_info "Infra still running. Ctrl+C to stop."
      wait
      exit 1
      ;;
  esac
  LAST_STATUS="$STATUS"
done

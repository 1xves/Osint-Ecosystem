#!/bin/bash
# ─── OSINT Pipeline — Trigger a Run ───────────────────────────────────────────
# Queues a pipeline run and polls for completion, printing live status updates.
#
# Usage:
#   ./run.sh                              — Philadelphia, United States (default)
#   ./run.sh "Austin" "United States"
#   ./run.sh "London" "United Kingdom"
# ──────────────────────────────────────────────────────────────────────────────

CITY=${1:-"Philadelphia"}
COUNTRY=${2:-"United States"}
PORT=${PORT:-8080}
API_KEY="df254048a96e4459b52191b8a07ee528c5cc39dfe91af4a3d58a01944ae0c861"
BASE_URL="http://localhost:$PORT"

# ── Colors ─────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ── Trigger run ────────────────────────────────────────────────────────────────
echo -e "${BOLD}Triggering OSINT run: ${CYAN}$CITY, $COUNTRY${NC}"

RESPONSE=$(curl -sf -X POST "$BASE_URL/runs" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d "{\"city_name\": \"$CITY\", \"country_or_region\": \"$COUNTRY\"}" 2>&1)

if [ $? -ne 0 ]; then
  echo -e "${RED}✗ Failed to reach API at $BASE_URL${NC}"
  echo -e "${DIM}Is ./start.sh running?${NC}"
  exit 1
fi

RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['run_id'])" 2>/dev/null)
DETAIL=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('detail',''))" 2>/dev/null)

if [ -z "$RUN_ID" ]; then
  echo -e "${RED}✗ API error: $DETAIL${NC}"
  echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
  exit 1
fi

echo -e "${GREEN}✓ Run queued${NC} — ${DIM}run_id: $RUN_ID${NC}"
echo ""

# ── Poll for status ────────────────────────────────────────────────────────────
LAST_STATUS=""
LAST_PHASE=""
ELAPSED=0
POLL_INTERVAL=10

while true; do
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))

  STATUS_JSON=$(curl -sf "$BASE_URL/runs/$RUN_ID" \
    -H "X-API-Key: $API_KEY" 2>/dev/null)

  if [ -z "$STATUS_JSON" ]; then
    echo -e "${DIM}[${ELAPSED}s] Could not reach API — retrying...${NC}"
    continue
  fi

  STATUS=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null)
  ENTITIES=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_entities_found',0))" 2>/dev/null)
  RELATIONSHIPS=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_relationships_found',0))" 2>/dev/null)

  # Print update only if something changed
  if [ "$STATUS" != "$LAST_STATUS" ] || [ "$((ELAPSED % 60))" -eq 0 ]; then
    case "$STATUS" in
      pending)
        echo -e "${DIM}[${ELAPSED}s] ⏳ Waiting for worker to pick up job...${NC}"
        ;;
      running)
        echo -e "${YELLOW}[${ELAPSED}s] 🔄 Running — entities: $ENTITIES | relationships: $RELATIONSHIPS${NC}"
        ;;
      complete)
        echo ""
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}  ✓ Run complete in ${ELAPSED}s${NC}"
        DURATION=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('duration_seconds','?'))" 2>/dev/null)
        echo -e "${GREEN}  Entities:      $ENTITIES${NC}"
        echo -e "${GREEN}  Relationships: $RELATIONSHIPS${NC}"
        echo -e "${GREEN}  Duration:      ${DURATION}s${NC}"
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo -e "Fetch briefing:"
        echo -e "${DIM}  curl -s $BASE_URL/runs/$RUN_ID/briefing -H 'X-API-Key: $API_KEY' | python3 -m json.tool${NC}"
        exit 0
        ;;
      failed)
        echo ""
        REASON=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('failure_reason','unknown'))" 2>/dev/null)
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${RED}  ✗ Run FAILED${NC}"
        echo -e "${RED}  Reason: $REASON${NC}"
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        exit 1
        ;;
    esac
    LAST_STATUS="$STATUS"
  fi
done

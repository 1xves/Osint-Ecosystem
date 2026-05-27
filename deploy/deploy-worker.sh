#!/bin/bash
# ─── Deploy dashboard to Cloudflare Worker ─────────────────────────────────────
# Usage: CF_TOKEN=<your-token> ./deploy-worker.sh
# Or:    ./deploy-worker.sh <your-token>
#
# Get your token at: https://dash.cloudflare.com/profile/api-tokens
# Token needs: "Edit Cloudflare Workers" permission
# ───────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ACCOUNT_ID="4d76f0f06af14b703892b764ae387edf"
WORKER_NAME="vk-osint-dashboard"
INDEX_HTML="$SCRIPT_DIR/index.html"

# ── Get token ──────────────────────────────────────────────────────────────────
CF_TOKEN="${1:-${CF_TOKEN:-}}"
if [[ -z "$CF_TOKEN" ]]; then
  echo "Error: Cloudflare API token required."
  echo "Usage: CF_TOKEN=<token> ./deploy-worker.sh"
  echo "Get one at: https://dash.cloudflare.com/profile/api-tokens (Edit Cloudflare Workers)"
  exit 1
fi

# ── Build Worker script ────────────────────────────────────────────────────────
echo "Building Worker script from $INDEX_HTML..."

ESCAPED_HTML=$(python3 -c "
import json, sys
html = open('$INDEX_HTML').read()
print(json.dumps(html))
")

WORKER_FILE=$(mktemp /tmp/worker_XXXXXX.js)

python3 - "$INDEX_HTML" "$WORKER_FILE" <<'PYEOF'
import sys, json
html = open(sys.argv[1]).read()
script = (
  'const HTML_CONTENT = ' + json.dumps(html) + ';\n\n'
  'addEventListener("fetch", event => {\n'
  '  event.respondWith(handleRequest(event.request));\n'
  '});\n\n'
  'async function handleRequest(request) {\n'
  '  const url = new URL(request.url);\n'
  '  if (url.pathname === "/" || url.pathname === "/index.html") {\n'
  '    return new Response(HTML_CONTENT, {\n'
  '      headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" }\n'
  '    });\n'
  '  }\n'
  '  return new Response("Not Found", { status: 404 });\n'
  '}\n'
)
open(sys.argv[2], 'w').write(script)
PYEOF

echo "  Worker size: $(wc -c < "$WORKER_FILE") bytes"

# ── Upload to Cloudflare ───────────────────────────────────────────────────────
echo "Deploying to Cloudflare Worker: $WORKER_NAME..."

RESPONSE=$(curl -s -X PUT \
  "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/workers/scripts/${WORKER_NAME}" \
  -H "Authorization: Bearer ${CF_TOKEN}" \
  -H "Content-Type: application/javascript" \
  --data-binary "@${WORKER_FILE}")

rm -f "$WORKER_FILE"

SUCCESS=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success', False))" 2>/dev/null)

if [[ "$SUCCESS" == "True" ]]; then
  echo ""
  echo "✓ Deployed successfully!"
  echo "  URL: https://vk-osint-dashboard.sylmobleyiii.workers.dev"
  echo "  Hard refresh (Cmd+Shift+R) to clear cache."
else
  echo ""
  echo "✗ Deployment failed:"
  echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
  exit 1
fi

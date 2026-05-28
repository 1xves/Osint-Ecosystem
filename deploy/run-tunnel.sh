#!/bin/bash
# deploy/run-tunnel.sh
# Wrapper script for launchd — starts the Cloudflare Tunnel.
# Called by com.finitebuilds.osint-tunnel.plist via launchd.
# Do NOT run this directly; use: ./services.sh start

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$PROJECT_DIR/deploy/cloudflared-config.yml"

exec /opt/homebrew/bin/cloudflared tunnel \
    --config "$CONFIG" \
    --no-autoupdate \
    run

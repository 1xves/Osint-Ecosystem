#!/bin/bash
# deploy/run-api.sh
# Wrapper script for launchd — starts the OSINT FastAPI server.
# Called by com.finitebuilds.osint-api.plist via launchd.
# The FastAPI app loads .env itself via pydantic-settings — no shell sourcing needed.

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

export PYTHONPATH="$PROJECT_DIR"

exec "$PROJECT_DIR/.venv/bin/uvicorn" osint.api.app:app \
    --host 127.0.0.1 \
    --port 8080 \
    --log-level info

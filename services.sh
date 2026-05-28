#!/bin/bash
# services.sh
# Manage OSINT backend services as macOS LaunchAgents.
# Services run in the background, start on login, and restart on crash.
#
# Usage:
#   ./services.sh install    — register services with launchd (run once)
#   ./services.sh uninstall  — remove services from launchd
#   ./services.sh start      — start all services now
#   ./services.sh stop       — stop all services now
#   ./services.sh restart    — stop then start
#   ./services.sh status     — show running status
#   ./services.sh logs       — tail live logs (Ctrl+C to stop)

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")/deploy" && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

API_LABEL="com.finitebuilds.osint-api"
TUNNEL_LABEL="com.finitebuilds.osint-tunnel"

API_PLIST="$DEPLOY_DIR/$API_LABEL.plist"
TUNNEL_PLIST="$DEPLOY_DIR/$TUNNEL_LABEL.plist"

API_LOG="$HOME/Library/Logs/osint-api.log"
TUNNEL_LOG="$HOME/Library/Logs/osint-tunnel.log"

# ── Helpers ───────────────────────────────────────────────────────────────────

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m  %s\n' "$*"; }
err()  { printf '  \033[31m✗\033[0m  %s\n' "$*"; }
info() { printf '  \033[34m→\033[0m  %s\n' "$*"; }

is_loaded() {
    launchctl print "$DOMAIN/$1" &>/dev/null
}

api_pid() {
    lsof -ti :8080 2>/dev/null | head -1
}

tunnel_pid() {
    pgrep -f "cloudflared tunnel" 2>/dev/null | head -1
}

_kill_orphans() {
    lsof -ti :8080 | xargs kill -9 2>/dev/null || true
    pkill -f "cloudflared tunnel" 2>/dev/null || true
}

# ── Commands ──────────────────────────────────────────────────────────────────

cmd_install() {
    bold "Installing OSINT services..."
    chmod +x "$DEPLOY_DIR/run-api.sh" "$DEPLOY_DIR/run-tunnel.sh"

    # Unload first if already present (idempotent install)
    launchctl bootout "$DOMAIN" "$LAUNCH_AGENTS_DIR/$API_LABEL.plist"    &>/dev/null || true
    launchctl bootout "$DOMAIN" "$LAUNCH_AGENTS_DIR/$TUNNEL_LABEL.plist" &>/dev/null || true
    _kill_orphans

    cp "$API_PLIST"    "$LAUNCH_AGENTS_DIR/"
    cp "$TUNNEL_PLIST" "$LAUNCH_AGENTS_DIR/"

    launchctl bootstrap "$DOMAIN" "$LAUNCH_AGENTS_DIR/$API_LABEL.plist"
    launchctl bootstrap "$DOMAIN" "$LAUNCH_AGENTS_DIR/$TUNNEL_LABEL.plist"

    ok "Services installed and will start automatically at every login."
    echo ""
    sleep 3
    cmd_status
}

cmd_uninstall() {
    bold "Uninstalling OSINT services..."
    launchctl bootout "$DOMAIN" "$LAUNCH_AGENTS_DIR/$API_LABEL.plist"    &>/dev/null || true
    launchctl bootout "$DOMAIN" "$LAUNCH_AGENTS_DIR/$TUNNEL_LABEL.plist" &>/dev/null || true
    rm -f "$LAUNCH_AGENTS_DIR/$API_LABEL.plist"
    rm -f "$LAUNCH_AGENTS_DIR/$TUNNEL_LABEL.plist"
    _kill_orphans
    ok "Services uninstalled."
}

cmd_start() {
    bold "Starting OSINT services..."
    _kill_orphans
    sleep 1

    for label in "$API_LABEL" "$TUNNEL_LABEL"; do
        if is_loaded "$label"; then
            launchctl kickstart "$DOMAIN/$label" &>/dev/null || true
        else
            err "$label not installed — run: ./services.sh install"
        fi
    done

    sleep 4
    cmd_status
}

cmd_stop() {
    bold "Stopping OSINT services..."
    for label in "$API_LABEL" "$TUNNEL_LABEL"; do
        if is_loaded "$label"; then
            launchctl kill SIGTERM "$DOMAIN/$label" &>/dev/null || true
        fi
    done
    _kill_orphans
    ok "Services stopped (launchd will restart them on next start or login)."
}

cmd_restart() {
    bold "Restarting OSINT services..."
    _kill_orphans
    for label in "$API_LABEL" "$TUNNEL_LABEL"; do
        if is_loaded "$label"; then
            launchctl kickstart -k "$DOMAIN/$label" &>/dev/null || true
        else
            err "$label not installed — run: ./services.sh install"
        fi
    done
    sleep 4
    cmd_status
}

cmd_status() {
    bold "Service Status"
    echo ""

    # API
    if is_loaded "$API_LABEL"; then
        PID=$(api_pid)
        if [[ -n "$PID" ]]; then
            if curl -sf http://127.0.0.1:8080/health &>/dev/null; then
                ok "API (PID $PID) — running and healthy on http://127.0.0.1:8080"
            else
                info "API (PID $PID) — process up but /health not responding yet"
            fi
        else
            info "API registered — starting up..."
        fi
    else
        err "API not installed — run: ./services.sh install"
    fi

    echo ""

    # Tunnel
    if is_loaded "$TUNNEL_LABEL"; then
        PID=$(tunnel_pid)
        if [[ -n "$PID" ]]; then
            ok "Tunnel (PID $PID) — api.finitebuilds.com → localhost:8080"
        else
            info "Tunnel registered — starting up..."
        fi
    else
        err "Tunnel not installed — run: ./services.sh install"
    fi

    echo ""
    bold "Logs"
    echo "  API:    tail -f $API_LOG"
    echo "  Tunnel: tail -f $TUNNEL_LOG"
    echo ""
    echo "  Run './services.sh logs' to tail both live."
}

cmd_logs() {
    bold "Tailing logs (Ctrl+C to stop)..."
    echo ""
    tail -f "$API_LOG" "$TUNNEL_LOG" 2>/dev/null \
        || tail -f "$API_LOG" 2>/dev/null \
        || echo "No log files yet — services may still be starting."
}

# ── Entry point ───────────────────────────────────────────────────────────────

CMD="${1:-help}"
case "$CMD" in
    install)   cmd_install   ;;
    uninstall) cmd_uninstall ;;
    start)     cmd_start     ;;
    stop)      cmd_stop      ;;
    restart)   cmd_restart   ;;
    status)    cmd_status    ;;
    logs)      cmd_logs      ;;
    *)
        bold "Usage: ./services.sh <command>"
        echo ""
        echo "  install    Register services with launchd (run once after setup)"
        echo "  uninstall  Remove services from launchd"
        echo "  start      Start all services now"
        echo "  stop       Stop all services now"
        echo "  restart    Stop then start (does NOT lose auto-start on login)"
        echo "  status     Show running status and health"
        echo "  logs       Tail live log output from both services"
        ;;
esac

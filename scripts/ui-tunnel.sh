#!/usr/bin/env bash
# Lightweight control for exposing the local Dagster UI via an ngrok fixed domain.
#
#   scripts/ui-tunnel.sh up        # start the tunnel in the background
#   scripts/ui-tunnel.sh down      # stop it
#   scripts/ui-tunnel.sh status    # up/down + public URL
#   scripts/ui-tunnel.sh restart
#
# Run on the SAME compute node where `dg dev` is running.
#
# Config via env (defaults in parens):
#   SC_UI_PORT          local Dagster UI port            (27182)
#   SC_UI_NGROK_DOMAIN  your reserved ngrok domain        (csj.ngrok.io)
#   SC_UI_BASIC_AUTH    "user:pass" -> adds --basic-auth  (unset = PUBLIC, no auth!)
set -euo pipefail

PORT="${SC_UI_PORT:-27182}"
DOMAIN="${SC_UI_NGROK_DOMAIN:-csj.ngrok.io}"
RUNDIR="${TMPDIR:-/tmp}/sc-curation-ui-tunnel"
PIDFILE="$RUNDIR/ngrok.pid"
LOGFILE="$RUNDIR/ngrok.log"
API="http://127.0.0.1:4040/api/tunnels"
mkdir -p "$RUNDIR"

have()       { command -v "$1" >/dev/null 2>&1; }
is_running() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }
public_url() {
  have curl || return 0
  curl -s --max-time 3 "$API" 2>/dev/null \
    | grep -o '"public_url":"https://[^"]*"' | head -1 \
    | sed 's/.*"https:/https:/; s/"$//'
}

up() {
  have ngrok || { echo "error: ngrok not found on PATH" >&2; exit 1; }
  if is_running; then
    echo "already up (pid $(cat "$PIDFILE")) -> $(public_url || true)"; return 0
  fi
  if have curl && ! curl -s --max-time 2 -o /dev/null "http://127.0.0.1:$PORT"; then
    echo "warning: nothing answering on 127.0.0.1:$PORT — start 'dg dev' first (UI will 502 until then)." >&2
  fi
  if [ -n "${SC_UI_BASIC_AUTH:-}" ]; then
    nohup ngrok http "$PORT" --domain="$DOMAIN" --basic-auth "$SC_UI_BASIC_AUTH" --log=stdout >"$LOGFILE" 2>&1 &
  else
    echo "WARNING: SC_UI_BASIC_AUTH not set -> https://$DOMAIN will be PUBLIC with NO login." >&2
    echo "         Anyone with the URL could launch/cancel runs. Set SC_UI_BASIC_AUTH=\"user:pass\" to protect it." >&2
    nohup ngrok http "$PORT" --domain="$DOMAIN" --log=stdout >"$LOGFILE" 2>&1 &
  fi
  echo $! >"$PIDFILE"
  for _ in $(seq 1 20); do          # tunnel can take a few s (IPv6 -> IPv4 fallback)
    sleep 1
    if ! is_running; then
      echo "error: ngrok exited early; last log lines:" >&2
      tail -n 15 "$LOGFILE" >&2; rm -f "$PIDFILE"; exit 1
    fi
    url="$(public_url || true)"
    if [ -n "$url" ]; then echo "up: $url  (pid $(cat "$PIDFILE"); log: $LOGFILE)"; return 0; fi
  done
  echo "started (pid $(cat "$PIDFILE")) but tunnel unconfirmed after 20s — check: tail -f $LOGFILE" >&2
}

down() {
  local stopped=0
  if is_running; then kill "$(cat "$PIDFILE")" 2>/dev/null && stopped=1 || true; fi
  if have pkill; then pkill -f "ngrok http $PORT --domain=$DOMAIN" 2>/dev/null && stopped=1 || true; fi
  rm -f "$PIDFILE"
  if [ "$stopped" = 1 ]; then echo "tunnel stopped."; else echo "no running tunnel found (on this node)."; fi
}

status() {
  if is_running; then echo "UP (pid $(cat "$PIDFILE")) -> $(public_url || echo "https://$DOMAIN")"
  else echo "DOWN"; fi
}

case "${1:-}" in
  up|start)  up ;;
  down|stop) down ;;
  status)    status ;;
  restart)   down; sleep 1; up ;;
  *) echo "usage: ${0##*/} {up|down|status|restart}" >&2; exit 2 ;;
esac

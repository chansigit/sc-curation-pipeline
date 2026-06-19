#!/usr/bin/env bash
# Run the Dagster UI (dg dev) in the BACKGROUND and expose it via an ngrok fixed domain.
# Cleanly starts/stops BOTH, including dg dev's child webserver/code-server processes.
#
#   scripts/serve-ui.sh up              # start dg dev (bg) + ngrok tunnel (bg)
#   scripts/serve-ui.sh down            # stop both (TERM the whole tree, then KILL)
#   scripts/serve-ui.sh status          # up/down + local + public URL
#   scripts/serve-ui.sh logs            # tail both logs
#   scripts/serve-ui.sh restart
#   scripts/serve-ui.sh dagster up|down # just the Dagster server
#   scripts/serve-ui.sh tunnel  up|down # just the ngrok tunnel
#
# Run on a Sherlock compute node (sh_dev / salloc), NOT the login node.
#
# Config via env (defaults in parens):
#   SC_UI_PORT          Dagster UI port                   (27182)
#   SC_UI_NGROK_DOMAIN  reserved ngrok domain              (csj.ngrok.io)
#   SC_UI_BASIC_AUTH    "user:pass" -> ngrok --basic-auth  (unset = PUBLIC, no auth!)
#   SC_UI_DG            path to the dg binary              (dl2025 venv)
# dg dev needs the watch dir: export SC_CURATION_WATCH_DIR=... or create a project .env.
set -euo pipefail

PORT="${SC_UI_PORT:-27182}"
DOMAIN="${SC_UI_NGROK_DOMAIN:-csj.ngrok.io}"
DG="${SC_UI_DG:-/scratch/users/chensj16/venvs/dl2025/.venv/bin/dg}"
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNDIR="${TMPDIR:-/tmp}/sc-curation-ui.$(id -u)"
DG_PID="$RUNDIR/dagster.pid"; DG_LOG="$RUNDIR/dagster.log"
NG_PID="$RUNDIR/ngrok.pid";   NG_LOG="$RUNDIR/ngrok.log"
DAGSTER_HOME_DIR="$PROJ/.dagster_home"   # gitignored; persists run history + registered samples
API="http://127.0.0.1:4040/api/tunnels"
mkdir -p "$RUNDIR"

have()    { command -v "$1" >/dev/null 2>&1; }
running() { [ -f "$1" ] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null; }
port_up() { have curl && curl -s --max-time 2 -o /dev/null "http://127.0.0.1:$PORT"; }
pub_url() {
  have curl || return 0
  curl -s --max-time 3 "$API" 2>/dev/null \
    | grep -o '"public_url":"https://[^"]*"' | head -1 | sed 's/.*"https:/https:/; s/"$//'
}

# Kill a process tree by PID (descendants first), never by command-line pattern.
kill_tree() {  # pid signal
  local pid="$1" sig="$2" c
  for c in $(pgrep -P "$pid" 2>/dev/null || true); do kill_tree "$c" "$sig"; done
  kill "-$sig" "$pid" 2>/dev/null || true
}
stop_svc() {  # pidfile name
  local pidf="$1" name="$2" pid
  if ! running "$pidf"; then rm -f "$pidf"; echo "$name: not running"; return 0; fi
  pid="$(cat "$pidf")"
  kill_tree "$pid" TERM
  for _ in $(seq 1 10); do sleep 1; kill -0 "$pid" 2>/dev/null || break; done
  if kill -0 "$pid" 2>/dev/null; then echo "$name: still up after TERM -> KILL"; kill_tree "$pid" KILL; sleep 1; fi
  rm -f "$pidf"; echo "$name: stopped"
}

start_dagster() {
  if running "$DG_PID"; then echo "dagster: already up (pid $(cat "$DG_PID")) http://127.0.0.1:$PORT"; return 0; fi
  [ -x "$DG" ] || { echo "error: dg not found at $DG (set SC_UI_DG)" >&2; exit 1; }
  if [ -z "${SC_CURATION_WATCH_DIR:-}" ] && [ ! -f "$PROJ/.env" ]; then
    echo "error: SC_CURATION_WATCH_DIR not set and no $PROJ/.env — dg dev would fail to load defs." >&2
    echo "       export SC_CURATION_WATCH_DIR=...  or:  cp .env.example .env" >&2; exit 1
  fi
  mkdir -p "$DAGSTER_HOME_DIR"
  ( cd "$PROJ" && exec env "DAGSTER_HOME=$DAGSTER_HOME_DIR" nohup "$DG" dev -p "$PORT" ) >"$DG_LOG" 2>&1 &
  echo $! >"$DG_PID"
  printf "dagster: starting (pid %s) " "$(cat "$DG_PID")"
  for _ in $(seq 1 45); do
    sleep 1; printf "."
    if ! kill -0 "$(cat "$DG_PID")" 2>/dev/null; then
      echo; echo "dagster: exited early — last log:" >&2; tail -n 20 "$DG_LOG" >&2; rm -f "$DG_PID"; exit 1
    fi
    if port_up; then echo; echo "dagster: UP on http://127.0.0.1:$PORT  (log: $DG_LOG)"; return 0; fi
  done
  echo; echo "dagster: not listening after 45s — check: tail -f $DG_LOG" >&2
}

start_ngrok() {
  if running "$NG_PID"; then echo "ngrok: already up -> $(pub_url || true)"; return 0; fi
  have ngrok || { echo "error: ngrok not on PATH" >&2; exit 1; }
  if [ -n "${SC_UI_BASIC_AUTH:-}" ]; then
    nohup ngrok http "$PORT" --domain="$DOMAIN" --basic-auth "$SC_UI_BASIC_AUTH" --log=stdout >"$NG_LOG" 2>&1 &
  else
    echo "WARNING: SC_UI_BASIC_AUTH not set -> https://$DOMAIN will be PUBLIC with no login." >&2
    echo "         Anyone with the URL could launch/cancel runs. Set SC_UI_BASIC_AUTH=\"user:pass\"." >&2
    nohup ngrok http "$PORT" --domain="$DOMAIN" --log=stdout >"$NG_LOG" 2>&1 &
  fi
  echo $! >"$NG_PID"
  for _ in $(seq 1 20); do
    sleep 1
    if ! kill -0 "$(cat "$NG_PID")" 2>/dev/null; then echo "ngrok: exited early — last log:" >&2; tail -n 15 "$NG_LOG" >&2; rm -f "$NG_PID"; exit 1; fi
    url="$(pub_url || true)"; [ -n "$url" ] && { echo "ngrok: $url"; return 0; }
  done
  echo "ngrok: started but tunnel unconfirmed after 20s — check: tail -f $NG_LOG" >&2
}

status() {
  if running "$DG_PID"; then echo "dagster: UP   (pid $(cat "$DG_PID"))  http://127.0.0.1:$PORT"; else echo "dagster: DOWN"; fi
  if running "$NG_PID"; then echo "ngrok:   UP   (pid $(cat "$NG_PID"))  $(pub_url || echo "https://$DOMAIN")"; else echo "ngrok:   DOWN"; fi
}
show_logs() {
  echo "===== dagster ($DG_LOG) ====="; tail -n 25 "$DG_LOG" 2>/dev/null || echo "(no log yet)"
  echo "===== ngrok ($NG_LOG) ====="; tail -n 25 "$NG_LOG" 2>/dev/null || echo "(no log yet)"
}
usage() { echo "usage: ${0##*/} {up|down|status|logs|restart | dagster up|down | tunnel up|down}" >&2; exit 2; }

case "${1:-}" in
  up)      start_dagster; start_ngrok ;;
  down)    stop_svc "$NG_PID" ngrok; stop_svc "$DG_PID" dagster ;;
  status)  status ;;
  logs)    show_logs ;;
  restart) stop_svc "$NG_PID" ngrok; stop_svc "$DG_PID" dagster; sleep 1; start_dagster; start_ngrok ;;
  dagster) case "${2:-}" in up|start) start_dagster ;; down|stop) stop_svc "$DG_PID" dagster ;; *) usage ;; esac ;;
  tunnel)  case "${2:-}" in up|start) start_ngrok ;; down|stop) stop_svc "$NG_PID" ngrok ;; *) usage ;; esac ;;
  *) usage ;;
esac

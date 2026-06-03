#!/bin/bash
# Slack-mention-trigger — runs the FastAPI server, the ngrok tunnel, and a
# watchdog loop in one Terminal window.
# - caffeinate prevents the Mac from sleeping while this script runs
# - watchdog restarts uvicorn / ngrok if either dies (checks every 30s)
# Ctrl+C in this Terminal stops everything cleanly.

cd "$(dirname "$0")"

if [[ ! -f .venv/bin/uvicorn ]]; then
    echo "venv missing. Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi
if [[ ! -f .env ]]; then
    echo ".env missing. Tokens not configured."
    exit 1
fi
if [[ ! -x ./bin/ngrok ]]; then
    echo "ngrok binary missing at ./bin/ngrok"
    exit 1
fi

# Capture children we spawn so the trap can kill them.
UVICORN_PID=""
NGROK_PID=""
CAFFEINATE_PID=""

cleanup() {
    echo ""
    echo "[$(date +%H:%M:%S)] shutting down..."
    # Kill children we spawned + any orphan uvicorn/ngrok on our port.
    for pid in "$CAFFEINATE_PID" "$UVICORN_PID" "$NGROK_PID"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null
    done
    pkill -f "uvicorn app:api --host 127.0.0.1 --port 8000" 2>/dev/null
    pkill -f "ngrok http 8000 --log=stdout" 2>/dev/null
    exit 0
}
trap cleanup EXIT INT TERM

start_uvicorn() {
    .venv/bin/uvicorn app:api --host 127.0.0.1 --port 8000 --log-level info 2>&1 \
      | sed 's/^/[uvicorn] /' &
    UVICORN_PID=$!
}

start_ngrok() {
    ./bin/ngrok http 8000 --log=stdout 2>&1 | sed 's/^/[ngrok]   /' &
    NGROK_PID=$!
}

echo "[$(date +%H:%M:%S)] preventing Mac sleep (caffeinate)..."
caffeinate -imsu &
CAFFEINATE_PID=$!

echo "[$(date +%H:%M:%S)] starting uvicorn on :8000..."
start_uvicorn
sleep 2

echo "[$(date +%H:%M:%S)] starting ngrok tunnel..."
start_ngrok
sleep 3

PUBLIC_URL=$(curl -s http://127.0.0.1:4040/api/tunnels 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tunnels'][0]['public_url'] if d.get('tunnels') else '?')" 2>/dev/null || echo "?")
echo ""
echo "================================================================"
echo "  Bot is running."
echo "  Public URL: $PUBLIC_URL"
echo "  Watchdog: checks every 30s, restarts on crash."
echo "  Sleep:    prevented while this Terminal is running."
echo "  Stop:     press Ctrl+C."
echo "================================================================"
echo ""

# Watchdog loop — runs until Ctrl+C.
while true; do
    sleep 30

    # uvicorn down? local /health no longer returns 200.
    if ! curl -fs -o /dev/null --max-time 5 http://127.0.0.1:8000/health 2>/dev/null; then
        echo "[$(date +%H:%M:%S)] [watchdog] uvicorn unreachable — restarting"
        pkill -f "uvicorn app:api --host 127.0.0.1 --port 8000" 2>/dev/null
        sleep 1
        start_uvicorn
        sleep 3
    fi

    # ngrok down? local inspector API gone.
    if ! curl -fs -o /dev/null --max-time 5 http://127.0.0.1:4040/api/tunnels 2>/dev/null; then
        echo "[$(date +%H:%M:%S)] [watchdog] ngrok unreachable — restarting"
        pkill -f "ngrok http 8000 --log=stdout" 2>/dev/null
        sleep 1
        start_ngrok
        sleep 3
    fi

    # caffeinate dead? respawn (rare, but bound it for safety).
    if ! kill -0 "$CAFFEINATE_PID" 2>/dev/null; then
        echo "[$(date +%H:%M:%S)] [watchdog] caffeinate died — respawning"
        caffeinate -imsu &
        CAFFEINATE_PID=$!
    fi
done

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v go >/dev/null 2>&1; then
  echo "go is required but not found."
  exit 1
fi

if [[ ! -f ".env" && -f ".env.example" ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Review values before production use."
fi

mkdir -p logs

run_service() {
  local name="$1"
  local logfile="$2"
  shift 2
  nohup "$@" >> "logs/$logfile" 2>&1 &
  local pid=$!
  echo "$pid $name" >> "logs/processes.pid"
  echo "Started $name (PID: $pid)"
}

: > "logs/processes.pid"

run_service "api" "api.log" go run ./cmd/api
run_service "watcher" "watcher.log" go run ./cmd/watcher

echo
echo "All services started."
echo "PID file: logs/processes.pid"
echo "Stop all: while read -r pid _; do kill \"\$pid\" 2>/dev/null || true; done < logs/processes.pid"

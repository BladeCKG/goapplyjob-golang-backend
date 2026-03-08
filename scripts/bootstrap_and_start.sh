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

db_url="${DATABASE_URL:-}"
if [[ -z "$db_url" && -f ".env" ]]; then
  db_url="$(grep -E '^DATABASE_URL=' .env | tail -n1 | cut -d'=' -f2- || true)"
fi
if [[ -z "$db_url" ]]; then
  echo "DATABASE_URL is required and must point to PostgreSQL."
  exit 1
fi
if [[ ! "$db_url" =~ ^postgres(ql)?:// ]]; then
  echo "DATABASE_URL must be a PostgreSQL URL (postgres:// or postgresql://)."
  echo "Current value: $db_url"
  exit 1
fi

mkdir -p logs

go run ./cmd/migrate

if [[ -f "logs/processes.pid" ]]; then
  while read -r pid _name; do
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo "Stopped previous process PID: $pid"
    fi
  done < "logs/processes.pid"
fi

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
run_service "importer" "importer.log" go run ./cmd/importer
run_service "rawjobworker" "rawjobworker.log" go run ./cmd/rawjobworker
run_service "parsedjobworker" "parsedjobworker.log" go run ./cmd/parsedjobworker

echo
echo "All services started."
echo "PID file: logs/processes.pid"
echo "Stop all: while read -r pid _; do kill \"\$pid\" 2>/dev/null || true; done < logs/processes.pid"

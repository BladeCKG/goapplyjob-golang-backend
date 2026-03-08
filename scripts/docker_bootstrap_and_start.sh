#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not found."
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
if [[ -z "$db_url" || ! "$db_url" =~ ^postgres(ql)?:// ]]; then
  echo "DATABASE_URL must be set to a PostgreSQL URL in .env before docker bootstrap."
  exit 1
fi

mkdir -p logs

docker compose run --rm api /app/migrate
docker compose --profile workers up -d --build

#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
cd "$REPO_ROOT"

echo "==> Go build all packages"
go build ./...

echo "==> Go test all packages"
go test ./...

if ! command -v docker >/dev/null 2>&1; then
  echo "==> Docker not found; skipping Docker image build checks"
  exit 0
fi

echo "==> Docker build api image"
docker build -f Dockerfile -t goapplyjob-api-precommit .

echo "==> Docker build workerchain image"
docker build -f Dockerfile.workerchain -t goapplyjob-workerchain-precommit .

echo "All pre-commit checks passed."

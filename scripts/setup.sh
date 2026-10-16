#!/usr/bin/env bash
# One-time local setup: prerequisites check, .env, Spark jars, container images.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "Docker is required (Docker Desktop or Docker Engine + compose plugin)"; exit 1; }
docker compose version >/dev/null || { echo "The docker compose plugin is required"; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker daemon is not running - start Docker Desktop first"; exit 1; }

mem=$(docker info --format '{{.MemTotal}}')
if (( mem < 7500000000 )); then
  echo "WARNING: Docker has $((mem / 1024 / 1024)) MiB of memory; at least 8 GB (better 10-12 GB) is recommended."
fi

[[ -f .env ]] || { cp .env.example .env; echo "Created .env from .env.example"; }

./scripts/fetch-jars.sh
./scripts/pull-images.sh
# The containers run as uid 185 (spark): results/ is a bind mount they write benchmark and
# maintenance reports into, which on Linux hosts is not writable unless we say so.
chmod -R a+rwX results
docker compose --profile tools build
echo "Setup complete. Next: ./scripts/run_demo.sh"

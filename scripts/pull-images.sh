#!/usr/bin/env bash
# Pulls every third-party image one by one, retrying each until it succeeds
# (Docker keeps already-downloaded layers between attempts).
set -uo pipefail
cd "$(dirname "$0")/.."
images=$(docker compose --profile tools --profile workload config --images | grep -v '^floe/' | sort -u)
images="$images apache/spark:3.5.6-scala2.12-java17-python3-ubuntu"
for img in $images; do
  for attempt in $(seq 1 60); do
    if docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img" >/dev/null 2>&1; then
      echo "ok   $img"; continue 2
    fi
    echo "retry $img ($attempt)"; sleep 5
  done
  echo "FAILED $img"; exit 1
done
echo "All images present."

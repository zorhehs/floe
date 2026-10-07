#!/usr/bin/env bash
# Helpers shared by the demo scripts.
set -euo pipefail
cd "$(dirname "$0")/../.."

DC="docker compose"

banner() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
note()   { printf '\033[0;33m-- %s\033[0m\n' "$*"; }

psql_run() {   # psql_run "SQL"
  printf '\033[0;32mpostgres>\033[0m %s\n' "$1"
  $DC exec -T postgres psql -U app -d oltp -v ON_ERROR_STOP=1 -c "$1"
}

trino_run() {  # trino_run "SQL"
  printf '\033[0;35mtrino>\033[0m %s\n' "$1"
  $DC exec -T trino trino --catalog lakehouse --schema shop --output-format ALIGNED --execute "$1"
}

trino_value() { # single value, no headers
  $DC exec -T trino trino --catalog lakehouse --schema shop --output-format TSV --execute "$1" 2>/dev/null | tail -n 1
}

wait_until() {  # wait_until "trino SQL returning one value" "expected" [timeout]
  local sql="$1" expected="$2" timeout="${3:-180}" start=$SECONDS value=""
  note "waiting for CDC: expecting '$expected'"
  while (( SECONDS - start < timeout )); do
    value="$(trino_value "$sql" || true)"
    if [[ "$value" == "$expected" ]]; then
      note "arrived in Iceberg after $(( SECONDS - start ))s"
      return 0
    fi
    sleep 2
  done
  echo "timed out after ${timeout}s (last value: '$value')" >&2
  return 1
}

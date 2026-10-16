#!/usr/bin/env bash
# Renders the Floe Console overview page to docs/console.png with headless Chrome
# (host only; the stack must be running and ideally the workload started).
#   [THEME=dark] ./scripts/screenshot-console.sh [page] [output]     page: overview (default), live, gdpr, ...
set -euo pipefail
cd "$(dirname "$0")/.."

PAGE="${1:-overview}"
OUT="${2:-docs/console.png}"
THEME="${THEME:-light}"          # THEME=dark for the dark mode
URL="http://localhost:8090/?theme=${THEME}#/${PAGE}"

CHROME="${CHROME:-}"
for candidate in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" google-chrome chromium chromium-browser; do
  [[ -n "$CHROME" ]] && break
  if [[ -x "$candidate" ]] || command -v "$candidate" >/dev/null 2>&1; then CHROME="$candidate"; fi
done
[[ -n "$CHROME" ]] || { echo "Chrome/Chromium not found; set CHROME=/path/to/chrome" >&2; exit 1; }
curl -fsS http://localhost:8090/api/ping >/dev/null || { echo "console not reachable on :8090 - run make up" >&2; exit 1; }

PROFILE="$(mktemp -d)"
trap 'rm -rf "$PROFILE"' EXIT
rm -f "$OUT"
# Chrome's exit status reflects its teardown, not the render: judge by the output file.
"$CHROME" --headless=new --disable-gpu --hide-scrollbars --no-first-run --no-default-browser-check \
  --user-data-dir="$PROFILE" --window-size=1440,1180 --virtual-time-budget=25000 \
  --screenshot="$OUT" "$URL" >/dev/null 2>&1 || true
[[ -s "$OUT" ]] || { echo "Chrome wrote no screenshot" >&2; exit 1; }
echo "wrote $OUT ($(wc -c <"$OUT" | tr -d ' ') bytes) from $URL"

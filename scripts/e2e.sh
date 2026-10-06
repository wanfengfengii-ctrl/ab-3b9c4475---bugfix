#!/usr/bin/env bash
# Full end-to-end verification:
#   1. clean stack (volumes wiped)
#   2. image build runs the Go unit tests inside the build stage
#   3. one-shot `verify` smoke: inserts data, appends earlier-timestamp
#      samples mid-pagination, asserts a stable snapshot and cursor errors;
#      it also parks a cursor in a shared volume
#   4. the app is restarted against the persisted database
#   5. `verify-resume` continues the parked cursor and checks the snapshot
#      is still pinned after the restart
#
# Exit code is non-zero if any of: unit tests, image build, API smoke,
# restart-resume checks fail.
set -euo pipefail

cd "$(dirname "$0")/.."

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  echo "docker compose (v2 or v1) is required" >&2
  exit 2
fi

export HOST_PORT="${HOST_PORT:-8080}"

cleanup() {
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
cleanup
trap cleanup EXIT

echo "== phase 1/3: build (unit tests run in the image build) + clean-start smoke =="
"${DC[@]}" up --build --abort-on-container-exit --exit-code-from verify verify

echo
echo "== phase 4: restart app with persisted data =="
"${DC[@]}" up -d app

app_cid="$("${DC[@]}" ps -q app)"
for _ in $(seq 1 60); do
  status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$app_cid" 2>/dev/null || echo none)"
  [ "$status" = "healthy" ] && break
  sleep 1
done
if [ "$status" != "healthy" ]; then
  echo "app did not become healthy after restart" >&2
  "${DC[@]}" logs app || true
  exit 1
fi

echo
echo "== phase 5: resume parked cursor after restart =="
"${DC[@]}" --profile resume run --rm verify-resume

echo
echo "E2E OK: unit tests, image build, API smoke and restart resume all passed"

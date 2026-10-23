#!/usr/bin/env bash
# A finite, real-traffic alert exercise; restore the default simulator on exit.
set -euo pipefail
cd "$(dirname "$0")/.."
probe="hw5-alert-probe-$(date +%s)-$$"
docker compose ps --status running --services | grep -qx simulator || {
  echo 'Start the default live stack before running this check.' >&2; exit 1;
}
simulator=$(docker compose ps -q simulator)
cleanup() {
  docker rm -f "$probe" >/dev/null 2>&1 || true
  docker start "$simulator" >/dev/null
}
trap cleanup EXIT
check() {
  docker compose --profile tools run --rm --no-deps tools python scripts/verify_monitoring.py \
    --alert-name "$1" --alert-state "$2" --timeout 180
}
docker compose stop simulator
docker compose run -d --no-deps --name "$probe" simulator python scripts/simulate.py --mode fraud --rate 15
check 'HW5 - High fraud ratio' firing
docker stop "$probe" >/dev/null
docker rm "$probe" >/dev/null
docker start "$simulator"
check 'HW5 - High fraud ratio' inactive
docker compose stop simulator
check 'HW5 - Scoring stream stalled' firing
docker start "$simulator"
check 'HW5 - Scoring stream stalled' inactive
echo 'Both Grafana alerts fired and recovered with real stream changes.'

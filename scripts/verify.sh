#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
docker compose up --build -d --wait --wait-timeout 300
docker compose --profile test run --rm --no-deps tests | tee reports/tests.txt
scorer=$(docker compose ps -q fraud_detector)
docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}}' "$scorer" > reports/scorer-before.txt
docker compose --profile tools run --rm --no-deps tools python scripts/verify_monitoring.py | tee reports/monitoring.txt
docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}}' "$(docker compose ps -q fraud_detector)" > reports/scorer-after.txt
diff reports/scorer-before.txt reports/scorer-after.txt
bash scripts/verify_alerts.sh | tee reports/alerts.txt
docker compose cp fraud_detector:/opt/project/storage/evidence/. reports/
docker compose ps -a > reports/compose-status.txt
echo 'Verified live queries, model activation without restart, and alert recovery.'

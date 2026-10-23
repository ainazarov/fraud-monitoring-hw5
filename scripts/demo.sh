#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose build seed
docker compose --profile airflow up -d --wait --wait-timeout 300
docker compose --profile test run --rm --no-deps tests
mkdir -p reports
docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}}' "$(docker compose ps -q fraud_detector)" > reports/scorer-before.txt
docker compose --profile tools run --rm --no-deps tools python scripts/e2e.py
docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}}' "$(docker compose ps -q fraud_detector)" > reports/scorer-after.txt
cmp reports/scorer-before.txt reports/scorer-after.txt
docker compose cp artifact-browser:/opt/project/storage/evidence/latest.json reports/e2e.json
docker compose cp artifact-browser:/opt/project/storage/dq_reports/latest.html reports/dq.html
docker compose ps -a > reports/compose-status.txt
printf '%s\n' 'Verified: all DAGs succeeded, new model scored transactions, scorer container did not restart.'

.PHONY: up down test verify retrain airflow logs

up:
	docker compose up --build -d --wait --wait-timeout 300

down:
	docker compose --profile airflow down

test:
	docker compose --profile test run --rm --no-deps tests

verify:
	docker compose --profile tools run --rm --no-deps tools python scripts/verify_monitoring.py

retrain:
	docker compose --profile tools run --rm --no-deps tools python scripts/retrain.py --advance-month

airflow:
	docker compose --profile airflow up -d --wait --wait-timeout 300

logs:
	docker compose logs -f --tail=100 fraud_detector scoring_writer simulator

#!/usr/bin/env bash
# Sourced by the official PostgreSQL entrypoint on first database initialization.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=reader_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE ROLE grafana_reader LOGIN;
ALTER ROLE grafana_reader PASSWORD :'reader_password';
GRANT CONNECT ON DATABASE airflow TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON TABLE scores TO grafana_reader;
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
SQL

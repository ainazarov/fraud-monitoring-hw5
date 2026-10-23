from urllib.parse import unquote, urlsplit
import os
import subprocess
import sys

from scripts.airflow_database_url import database_url


def test_password_punctuation_cannot_change_connection_destination():
    environment = {"PGUSER": "airflow", "PGPASSWORD": "example:@/?#%+word",
                   "PGDATABASE": "airflow", "PGHOST": "postgres", "PGPORT": "5432"}
    parsed = urlsplit(database_url(environment))
    assert parsed.hostname == "postgres"
    assert parsed.port == 5432
    assert unquote(parsed.password) == environment["PGPASSWORD"]
    assert parsed.path == "/airflow"


def test_airflow_command_output_contains_no_trailing_newline():
    environment = {"PGUSER": "airflow", "PGPASSWORD": "example:@/?#%+word",
                   "PGDATABASE": "airflow", "PGHOST": "postgres", "PGPORT": "5432"}
    result = subprocess.run([sys.executable, "-m", "scripts.airflow_database_url"],
                            env={**os.environ, **environment}, capture_output=True, text=True, check=True)
    assert result.stdout == database_url(environment)

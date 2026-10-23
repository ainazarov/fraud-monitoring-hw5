"""Build Airflow's connection URI without interpreting password punctuation."""
import os
import sys
from urllib.parse import quote


def database_url(environment):
    user = quote(environment["PGUSER"], safe="")
    password = quote(environment["PGPASSWORD"], safe="")
    database = quote(environment["PGDATABASE"], safe="")
    return f"postgresql+psycopg2://{user}:{password}@{environment['PGHOST']}:{environment['PGPORT']}/{database}"


if __name__ == "__main__":
    # Airflow captures stdout internally via its supported configuration _CMD.
    sys.stdout.write(database_url(os.environ))

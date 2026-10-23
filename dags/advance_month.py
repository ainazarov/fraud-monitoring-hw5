"""Monthly supply simulator: append and prune commit together, once per DAG run."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.data import advance_month

with DAG(
    dag_id="advance_month", start_date=datetime(2024, 1, 1), schedule="*/10 * * * *",
    catchup=False, max_active_runs=1,
    default_args={"owner": "fraud-mlops", "retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["fraud", "data", "calendar-month"],
    doc_md=__doc__,
) as dag:
    advance_calendar_month = PythonOperator(
        task_id="advance_calendar_month", python_callable=advance_month,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

"""Publish the DQ report first; a separate task makes invalid data fail visibly."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality import assert_quality, run_quality_check

with DAG(
    dag_id="data_quality", start_date=datetime(2024, 1, 1), schedule="*/5 * * * *",
    catchup=False, max_active_runs=1,
    default_args={"owner": "fraud-mlops", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["fraud", "data-quality"], doc_md=__doc__,
) as dag:
    check_dataset = PythonOperator(
        task_id="check_dataset", python_callable=run_quality_check,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
    enforce_quality = PythonOperator(
        task_id="enforce_quality", python_callable=assert_quality,
        op_kwargs={"report": check_dataset.output},
    )
    check_dataset >> enforce_quality

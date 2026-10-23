"""Every five minutes, evaluate and safely publish the next temporal window."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.training import (
    evaluate_candidate, prepare_window, publish_candidate, train_candidate,
)


with DAG(
    dag_id="retrain_model",
    description="Three-month temporal training with a two-metric promotion gate",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fraud-ml", "retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["fraud", "training", "quality-gate"],
) as dag:
    prepare = PythonOperator(task_id="prepare_window", python_callable=prepare_window)
    train = PythonOperator(task_id="train_candidate", python_callable=train_candidate)
    evaluate = PythonOperator(task_id="evaluate_candidate", python_callable=evaluate_candidate)
    publish = PythonOperator(task_id="publish_candidate", python_callable=publish_candidate)
    prepare >> train >> evaluate >> publish

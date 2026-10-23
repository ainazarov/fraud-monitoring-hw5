"""Parse real DAG modules and verify executable orchestration contracts."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    # Parsing must not need a configured metadata DB, running services, or a mart.
    return DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)


def test_dags_import_without_errors(dag_bag):
    assert dag_bag.import_errors == {}
    assert set(dag_bag.dags) == {"advance_month", "data_quality", "retrain_model"}


@pytest.mark.parametrize("dag_id,schedule", [
    ("advance_month", "*/10 * * * *"),
    ("data_quality", "*/5 * * * *"),
    ("retrain_model", "*/5 * * * *"),
])
def test_schedules_do_not_catch_up_or_overlap(dag_bag, dag_id, schedule):
    dag = dag_bag.dags[dag_id]
    assert dag.schedule_interval == schedule
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date is not None


def test_retraining_dependencies_and_external_documented_task_functions(dag_bag):
    dag = dag_bag.dags["retrain_model"]
    chain = ["prepare_window", "train_candidate", "evaluate_candidate", "publish_candidate"]
    assert set(dag.task_ids) == set(chain)
    for index, task_id in enumerate(chain):
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == ({chain[index + 1]} if index + 1 < len(chain) else set())
        assert task.python_callable.__module__.startswith("pipeline.")
        assert task.python_callable.__doc__
        assert task.retries >= 1


def test_quality_report_precedes_failure_enforcement(dag_bag):
    dag = dag_bag.dags["data_quality"]
    assert set(dag.task_ids) == {"check_dataset", "enforce_quality"}
    check = dag.get_task("check_dataset")
    enforce = dag.get_task("enforce_quality")
    assert check.downstream_task_ids == {"enforce_quality"}
    assert enforce.upstream_task_ids == {"check_dataset"}
    assert enforce.trigger_rule == "all_success"
    assert "run_id" in check.op_kwargs


def test_monthly_task_uses_durable_airflow_run_identifier(dag_bag):
    task = dag_bag.dags["advance_month"].get_task("advance_calendar_month")
    assert task.op_kwargs["run_id"] == "{{ run_id }}"
    assert task.python_callable.__module__ == "pipeline.data"
    assert task.python_callable.__name__ == "advance_month"

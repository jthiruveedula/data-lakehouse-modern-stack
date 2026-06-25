"""Maintenance DAG — weekly Delta OPTIMIZE/VACUUM and Iceberg compaction."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = Variable.get("gcp_project_id", default_var="my-project")
REGION = Variable.get("gcp_region", default_var="us-central1")
GCS_BUCKET = Variable.get("lakehouse_gcs_bucket", default_var="my-lakehouse")
SERVICE_ACCOUNT = Variable.get(
    "dataproc_service_account",
    default_var=f"dataproc-lakehouse@{PROJECT_ID}.iam.gserviceaccount.com",
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DELTA_TABLES = ["bronze/events", "silver/customers", "silver/orders"]
ICEBERG_TABLES = ["bronze/events_iceberg"]


def _make_batch(script: str, args: list[str]) -> dict:
    return {
        "runtime_config": {"version": "2.1"},
        "environment_config": {"execution_config": {"service_account": SERVICE_ACCOUNT}},
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/jobs/{script}",
            "args": args,
        },
    }


def _log_maintenance_start(**context: dict) -> None:
    print(f"Starting weekly maintenance — run_id={context['run_id']}")


def _log_maintenance_complete(**context: dict) -> None:
    print(f"Maintenance complete — run_id={context['run_id']}")


with DAG(
    dag_id="weekly_maintenance",
    description="Weekly Delta OPTIMIZE/VACUUM + Iceberg compaction + cost report",
    schedule="0 2 * * 0",  # every Sunday at 02:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "maintenance", "weekly"],
    doc_md="""
## Weekly Maintenance DAG

Runs every Sunday at 02:00 UTC.

### Tasks
- **Delta OPTIMIZE** — Z-order clustering on all Bronze and Silver Delta tables
- **Delta VACUUM** — Delete files older than 7 days retention threshold
- **Iceberg Compaction** — Rewrite small data files, expire old snapshots
- **Cost Report** — Generate BigQuery cost analysis and push to GCS
""",
) as dag:
    log_start = PythonOperator(
        task_id="log_start",
        python_callable=_log_maintenance_start,
    )

    # ── Delta maintenance ─────────────────────────────────────────────────────
    delta_optimize = DataprocCreateBatchOperator(
        task_id="delta_optimize_and_vacuum",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_make_batch(
            "delta_maintenance_job.py",
            ["--tables", ",".join(DELTA_TABLES), "--vacuum-hours=168"],
        ),
        batch_id="delta-maint-{{ ds_nodash }}",
    )

    # ── Iceberg compaction ────────────────────────────────────────────────────
    iceberg_compact = DataprocCreateBatchOperator(
        task_id="iceberg_compaction",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_make_batch(
            "iceberg_maintenance_job.py",
            ["--tables", ",".join(ICEBERG_TABLES), "--snapshots-to-keep=10"],
        ),
        batch_id="iceberg-maint-{{ ds_nodash }}",
    )

    # ── Cost report ───────────────────────────────────────────────────────────
    cost_report = DataprocCreateBatchOperator(
        task_id="generate_cost_report",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_make_batch(
            "cost_report_job.py",
            ["--project", PROJECT_ID, "--output-bucket", GCS_BUCKET],
        ),
        batch_id="cost-report-{{ ds_nodash }}",
    )

    log_complete = PythonOperator(
        task_id="log_complete",
        python_callable=_log_maintenance_complete,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Dependencies ──────────────────────────────────────────────────────────
    log_start >> [delta_optimize, iceberg_compact, cost_report] >> log_complete >> end

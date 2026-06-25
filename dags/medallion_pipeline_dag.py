"""Medallion Pipeline DAG — daily Bronze → Silver → Gold refresh on Cloud Composer."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# ── Config from Airflow Variables ─────────────────────────────────────────────
PROJECT_ID = Variable.get("gcp_project_id", default_var="my-project")
REGION = Variable.get("gcp_region", default_var="us-central1")
GCS_BUCKET = Variable.get("lakehouse_gcs_bucket", default_var="my-lakehouse")
SERVICE_ACCOUNT = Variable.get(
    "dataproc_service_account",
    default_var=f"dataproc-lakehouse@{PROJECT_ID}.iam.gserviceaccount.com",
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

BATCH_RUNTIME = {
    "runtime_config": {"version": "2.1"},
    "environment_config": {"execution_config": {"service_account": SERVICE_ACCOUNT}},
}


def _pyspark_batch(job_script: str, args: list[str]) -> dict:
    return {
        **BATCH_RUNTIME,
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/jobs/{job_script}",
            "args": args,
            "jar_file_uris": [
                f"gs://{GCS_BUCKET}/jars/delta-spark_2.12-3.1.0.jar",
                f"gs://{GCS_BUCKET}/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
            ],
        },
    }


def _notify_success(**context: dict) -> None:
    dag_run = context["dag_run"]
    logical_date = context["logical_date"].date().isoformat()
    print(f"Pipeline completed successfully for {logical_date} — run_id={dag_run.run_id}")


with DAG(
    dag_id="medallion_pipeline",
    description="Daily Bronze → Silver → Gold lakehouse refresh",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "medallion", "production"],
    doc_md="""
## Medallion Pipeline

Runs the full Bronze → Silver → Gold refresh pipeline daily at 04:00 UTC.

### Stages
1. **Bronze Ingest** — Pull REST API + Kafka CDC into raw Delta tables
2. **Silver Transform** — Dedup + SCD Type 2 MERGE for customers and orders
3. **dbt Gold** — Aggregated revenue and customer LTV models
4. **Notify** — Log completion status

All Spark jobs run as Dataproc Serverless batches.
""",
) as dag:
    start = EmptyOperator(task_id="start")

    # ── Bronze ────────────────────────────────────────────────────────────────
    bronze_ingest = DataprocCreateBatchOperator(
        task_id="bronze_ingest",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_pyspark_batch("bronze_ingest_job.py", ["--date={{ ds }}"]),
        batch_id="bronze-{{ ds_nodash }}",
    )

    wait_for_bronze = GCSObjectExistenceSensor(
        task_id="wait_for_bronze_marker",
        bucket=GCS_BUCKET,
        object="bronze/_markers/{{ ds }}/_SUCCESS",
        timeout=3600,
        poke_interval=60,
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    silver_customers = DataprocCreateBatchOperator(
        task_id="silver_customers",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_pyspark_batch(
            "silver_transform_job.py",
            ["--table=customers", "--date={{ ds }}"],
        ),
        batch_id="silver-customers-{{ ds_nodash }}",
    )

    silver_orders = DataprocCreateBatchOperator(
        task_id="silver_orders",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_pyspark_batch(
            "silver_transform_job.py",
            ["--table=orders", "--date={{ ds }}"],
        ),
        batch_id="silver-orders-{{ ds_nodash }}",
    )

    # ── dbt Gold ──────────────────────────────────────────────────────────────
    dbt_gold = DataprocCreateBatchOperator(
        task_id="dbt_gold_run",
        project_id=PROJECT_ID,
        region=REGION,
        batch=_pyspark_batch(
            "dbt_runner_job.py",
            ["--models=gold", "--target=prod", "--vars", "run_date:{{ ds }}"],
        ),
        batch_id="dbt-gold-{{ ds_nodash }}",
    )

    # ── Notify ────────────────────────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        start
        >> bronze_ingest
        >> wait_for_bronze
        >> [silver_customers, silver_orders]
        >> dbt_gold
        >> notify
        >> end
    )

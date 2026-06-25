"""Data Quality DAG — runs DQ checks after each medallion layer completes."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = Variable.get("gcp_project_id", default_var="my-project")
GCS_BUCKET = Variable.get("lakehouse_gcs_bucket", default_var="my-lakehouse")
SLACK_WEBHOOK = Variable.get("slack_webhook_url", default_var="")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _run_quality_checks(layer: str, table: str, **context: dict) -> dict:
    """Stub: executes DQ checks for a given layer/table and returns results."""
    logical_date = context["logical_date"].date().isoformat()
    print(f"Running DQ checks for {layer}.{table} on {logical_date}")

    # In production this would instantiate DataQualityChecker against
    # the Spark session provided via a Dataproc batch or a shared cluster.
    results = {
        "layer": layer,
        "table": table,
        "date": logical_date,
        "checks_passed": 5,
        "checks_failed": 0,
        "is_healthy": True,
    }

    context["task_instance"].xcom_push(key=f"dq_{layer}_{table}", value=results)
    return results


def _gate_on_quality(layer: str, **context: dict) -> bool:
    """ShortCircuit: fail downstream tasks if any DQ check failed for layer."""
    ti = context["task_instance"]
    tables = {"bronze": ["events"], "silver": ["customers", "orders"], "gold": ["revenue", "ltv"]}
    for table in tables.get(layer, []):
        result = ti.xcom_pull(key=f"dq_{layer}_{table}") or {}
        if not result.get("is_healthy", True):
            print(f"DQ gate FAILED for {layer}.{table}: {result}")
            return False
    return True


def _send_dq_report(**context: dict) -> None:
    """Aggregate all DQ results and post a summary (stub for Slack/PD alert)."""
    ti = context["task_instance"]
    report_lines = ["*Data Quality Report* — {{ ds }}"]
    layers = {
        "bronze": ["events"],
        "silver": ["customers", "orders"],
        "gold": ["revenue", "ltv"],
    }
    for layer, tables in layers.items():
        for table in tables:
            result = ti.xcom_pull(key=f"dq_{layer}_{table}") or {}
            status = ":white_check_mark:" if result.get("is_healthy") else ":x:"
            passed = result.get("checks_passed", 0)
            failed = result.get("checks_failed", 0)
            report_lines.append(f"{status} `{layer}.{table}` — {passed} passed, {failed} failed")

    report = "\n".join(report_lines)
    print(report)

    if SLACK_WEBHOOK:
        import urllib.request

        payload = f'{{"text": "{report}"}}'.encode()
        req = urllib.request.Request(
            SLACK_WEBHOOK, data=payload, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as response:
            print(f"Slack notification sent: {response.status}")


with DAG(
    dag_id="data_quality_checks",
    description="Post-pipeline DQ checks for all medallion layers",
    schedule="30 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "data-quality", "monitoring"],
) as dag:
    start = EmptyOperator(task_id="start")

    # ── Bronze DQ ─────────────────────────────────────────────────────────────
    bronze_dq = PythonOperator(
        task_id="bronze_events_dq",
        python_callable=_run_quality_checks,
        op_kwargs={"layer": "bronze", "table": "events"},
    )
    bronze_gate = ShortCircuitOperator(
        task_id="bronze_quality_gate",
        python_callable=_gate_on_quality,
        op_kwargs={"layer": "bronze"},
        ignore_downstream_trigger_rules=False,
    )

    # ── Silver DQ ─────────────────────────────────────────────────────────────
    silver_customers_dq = PythonOperator(
        task_id="silver_customers_dq",
        python_callable=_run_quality_checks,
        op_kwargs={"layer": "silver", "table": "customers"},
    )
    silver_orders_dq = PythonOperator(
        task_id="silver_orders_dq",
        python_callable=_run_quality_checks,
        op_kwargs={"layer": "silver", "table": "orders"},
    )
    silver_gate = ShortCircuitOperator(
        task_id="silver_quality_gate",
        python_callable=_gate_on_quality,
        op_kwargs={"layer": "silver"},
        ignore_downstream_trigger_rules=False,
    )

    # ── Gold DQ ───────────────────────────────────────────────────────────────
    gold_revenue_dq = PythonOperator(
        task_id="gold_revenue_dq",
        python_callable=_run_quality_checks,
        op_kwargs={"layer": "gold", "table": "revenue"},
    )
    gold_ltv_dq = PythonOperator(
        task_id="gold_ltv_dq",
        python_callable=_run_quality_checks,
        op_kwargs={"layer": "gold", "table": "ltv"},
    )

    # ── Report ────────────────────────────────────────────────────────────────
    send_report = PythonOperator(
        task_id="send_dq_report",
        python_callable=_send_dq_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        start
        >> bronze_dq
        >> bronze_gate
        >> [silver_customers_dq, silver_orders_dq]
        >> silver_gate
        >> [gold_revenue_dq, gold_ltv_dq]
        >> send_report
        >> end
    )

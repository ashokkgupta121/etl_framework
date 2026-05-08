# =============================================================================
# FILE    : airflow/dag_etl_batch_runner.py
# PURPOSE : Reusable Airflow DAG template for any ETL batch.
#           One DAG file per batch_job_config — parameterised via
#           BATCH_CONFIG at the top of each DAG file.
#           Dependency between tables is handled inside BatchOrchestrator
#           (via execution_order + depends_on_job_ids in job_config).
#           Airflow manages the batch-level schedule; Databricks manages
#           the table-level parallelism.
#
# USAGE   : Copy this template and set BATCH_CONFIG for each new batch.
#           See dag_visionplus_vp1.py for a concrete example.
# =============================================================================

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from typing import Any
import logging

log = logging.getLogger(__name__)


# =============================================================================
# ── BATCH CONFIGURATION ── edit this section for each new batch ──
# =============================================================================

BATCH_CONFIG = {
    "batch_job_config_id":  1,
    "batch_name":           "VP-1",
    "source_system":        "VisionPlus",
    "dag_id":               "dag_visionplus_vp1",
    "schedule_interval":    "0 2 * * *",        # 02:00 UTC daily
    "start_date":           days_ago(1),
    "catchup":              False,
    "max_active_runs":      1,                  # Prevent overlapping runs
    "databricks_conn_id":   "databricks_default",
    "databricks_job_id":    "YOUR_DATABRICKS_JOB_ID",   # Replace with actual job ID
    "tags":                 ["etl", "visionplus", "bronze"],
    "alert_email":          ["etl-alerts@yourcompany.com"],
    "retry_count":          2,
    "retry_delay_minutes":  5,
}


# =============================================================================
# DAG DEFAULT ARGS
# =============================================================================

default_args = {
    "owner":              "etl-framework",
    "depends_on_past":    False,
    "email_on_failure":   True,
    "email_on_retry":     False,
    "email":              BATCH_CONFIG["alert_email"],
    "retries":            BATCH_CONFIG["retry_count"],
    "retry_delay":        timedelta(minutes=BATCH_CONFIG["retry_delay_minutes"]),
}


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id            = BATCH_CONFIG["dag_id"],
    default_args      = default_args,
    description       = f"ETL batch: {BATCH_CONFIG['batch_name']} — {BATCH_CONFIG['source_system']}",
    schedule_interval = BATCH_CONFIG["schedule_interval"],
    start_date        = BATCH_CONFIG["start_date"],
    catchup           = BATCH_CONFIG["catchup"],
    max_active_runs   = BATCH_CONFIG["max_active_runs"],
    tags              = BATCH_CONFIG["tags"],
) as dag:

    # ── Task: pre-flight validation (optional Python check) ──────────────────
    def pre_flight_check(**context) -> str:
        """
        Optional pre-flight: can check upstream data availability,
        validate business date, or gate on a control table.
        Returns 'run_batch' to proceed or 'skip_batch' to abort.
        """
        business_date = context["ds"]           # Airflow logical date YYYY-MM-DD
        dag_run_id    = context["run_id"]
        log.info("Pre-flight check: business_date=%s  dag_run_id=%s", business_date, dag_run_id)
        # Add any gate logic here (e.g. check source availability)
        return "run_batch"

    pre_flight = BranchPythonOperator(
        task_id         = "pre_flight_check",
        python_callable = pre_flight_check,
        provide_context = True,
    )

    skip_batch = EmptyOperator(task_id="skip_batch")

    # ── Task: run batch via Databricks Job ───────────────────────────────────
    # Databricks job is the entry notebook (entry_batch_runner.py).
    # business_date and batch_job_config_id are passed as job parameters.
    run_batch = DatabricksRunNowOperator(
        task_id          = "run_batch",
        databricks_conn_id = BATCH_CONFIG["databricks_conn_id"],
        job_id           = BATCH_CONFIG["databricks_job_id"],
        notebook_params  = {
            "batch_job_config_id": str(BATCH_CONFIG["batch_job_config_id"]),
            "business_date":       "{{ ds }}",          # Airflow template: YYYY-MM-DD
            "trigger_type":        "SCHEDULED",
            "airflow_dag_id":      "{{ run_id }}",
        },
    )

    # ── Task: post-batch summary (optional) ──────────────────────────────────
    def post_batch_summary(**context) -> None:
        """Logs completion summary. Can be extended to send Slack/Teams alerts."""
        log.info(
            "Batch '%s' completed for business_date=%s",
            BATCH_CONFIG["batch_name"],
            context["ds"]
        )

    post_summary = PythonOperator(
        task_id         = "post_batch_summary",
        python_callable = post_batch_summary,
        provide_context = True,
    )

    end = EmptyOperator(
        task_id             = "end",
        trigger_rule        = "none_failed_min_one_success",
    )

    # ── DAG dependency graph ──────────────────────────────────────────────────
    #
    #   pre_flight_check
    #       ├── run_batch → post_batch_summary → end
    #       └── skip_batch                    → end
    #
    pre_flight >> [run_batch, skip_batch]
    run_batch  >> post_summary >> end
    skip_batch >> end

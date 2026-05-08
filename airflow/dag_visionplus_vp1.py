# =============================================================================
# FILE    : airflow/dag_visionplus_vp1.py
# PURPOSE : Airflow DAG for VisionPlus Batch VP-1
#           Schedule: Daily at 02:00 UTC
#           Tables  : ATH2_LRPMT_TGT (FULL), ATPTRPT_TGT (INC),
#                     ATH3X_TGT (INC), ATH3D_TGT (FULL)
#
# TABLE DEPENDENCY MODEL:
#   Wave 1 (execution_order=1) — parallel:  ATH2_LRPMT, ATPTRPT, ATH3X
#   Wave 2 (execution_order=2) — sequential: ATH3D (depends on wave 1)
#
#   This dependency is declared in job_config.execution_order and
#   job_config.depends_on_job_ids — Airflow does NOT model it.
#   Airflow triggers ONE Databricks job; BatchOrchestrator handles waves.
# =============================================================================

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner":            "etl-framework",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email":            ["etl-alerts@yourcompany.com"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id            = "dag_visionplus_vp1",
    default_args      = default_args,
    description       = "VisionPlus VP-1 daily batch — Bronze layer",
    schedule_interval = "0 2 * * *",
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["etl", "visionplus", "bronze", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_vp1_batch = DatabricksRunNowOperator(
        task_id            = "run_vp1_batch",
        databricks_conn_id = "databricks_default",
        job_id             = "{{ var.value.DATABRICKS_JOB_ID_VP1 }}",   # set in Airflow Variables
        notebook_params    = {
            "batch_job_config_id": "1",
            "business_date":       "{{ ds }}",
            "trigger_type":        "SCHEDULED",
            "airflow_dag_id":      "{{ run_id }}",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_vp1_batch >> end

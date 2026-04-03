#databricks command
# =============================================================================
# FILE    : framework/entry_batch_runner.py
# PURPOSE : Databricks entry notebook / job script.
#           This is the SINGLE entry point that Airflow triggers via
#           DatabricksRunNowOperator. It receives parameters, initialises
#           the framework, and calls BatchOrchestrator.run().
#
# AIRFLOW PARAMETERS RECEIVED:
#   - batch_job_config_id : int   (e.g. 1 for VP-1)
#   - business_date       : str   (YYYY-MM-DD, from Airflow {{ ds }})
#   - trigger_type        : str   (SCHEDULED | MANUAL | RERUN)
#   - airflow_dag_id      : str   (Airflow run_id for traceability)
#
# DATABRICKS SETUP:
#   Attach this file as the "Entry point notebook" in the Databricks Job.
#   Library dependencies (framework/modules/) must be on the cluster
#   via an init script or by adding framework/modules to sys.path.
# =============================================================================

import sys
import os
import logging
from datetime import date

# -- Make framework modules importable ---------------------------------------
# In Databricks, the repo is mounted at /Workspace/Repos/<user>/etl_framework
REPO_ROOT = "/Workspace/Repos/etl_framework"
sys.path.insert(0, f"{REPO_ROOT}/framework/modules")

# -- Configure logging -------------------------------------------------------
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("etl.entry_batch_runner")

# -- Import framework modules ------------------------------------------------
from session_manager     import SessionManager
from secret_manager      import SecretManager
from batch_orchestrator  import BatchOrchestrator
from app_config          import EnvironmentConfig, TriggerType

# ===========================================================================
# STEP 1 — Resolve Databricks widgets / job parameters
# ===========================================================================

try:
    # In a Databricks notebook context:
    dbutils.widgets.text("batch_job_config_id", "1")
    dbutils.widgets.text("business_date",        "")
    dbutils.widgets.text("trigger_type",         "SCHEDULED")
    dbutils.widgets.text("airflow_dag_id",        "manual_run")
except Exception:
    pass  # Running outside Databricks (unit test / CI)

sm_temp = SessionManager()
spark_temp = sm_temp.get_or_create_session()

batch_job_config_id = int(
    SessionManager.resolve_widget_param(spark_temp, "batch_job_config_id", "1")
)
business_date_str = SessionManager.resolve_widget_param(
    spark_temp, "business_date", str(date.today())
)
trigger_type   = SessionManager.resolve_widget_param(spark_temp, "trigger_type",  "SCHEDULED")
airflow_dag_id = SessionManager.resolve_widget_param(spark_temp, "airflow_dag_id", "manual_run")

business_date = date.fromisoformat(business_date_str)

logger.info(
    "Entry params — batch_job_config_id=%s  business_date=%s  trigger=%s  dag=%s",
    batch_job_config_id, business_date, trigger_type, airflow_dag_id
)

# ===========================================================================
# STEP 2 — Initialise session context
# ===========================================================================

env_config = EnvironmentConfig()
session_mgr = SessionManager(env_config)
spark       = session_mgr.get_or_create_session()

from audit_manager import AuditManager
batch_log_id = AuditManager.generate_batch_log_id(batch_job_config_id)

session_mgr.set_run_context(
    business_date       = business_date,
    batch_job_config_id = batch_job_config_id,
    batch_job_log_id    = batch_log_id,
    trigger_type        = trigger_type,
)

# ===========================================================================
# STEP 3 — Run the batch
# ===========================================================================

try:
    orchestrator = BatchOrchestrator(spark=spark, dbutils=dbutils)
    final_status = orchestrator.run(
        batch_job_config_id = batch_job_config_id,
        business_date       = business_date,
        airflow_dag_id      = airflow_dag_id,
        trigger_type        = trigger_type,
    )
    logger.info("Batch run finished — final_status=%s", final_status)

    # Exit code used by Databricks Jobs to determine success/failure
    if final_status == "FAILED":
        raise RuntimeError(f"Batch {batch_job_config_id} FAILED for business_date={business_date}.")

except Exception as exc:
    logger.exception("Unhandled exception in entry_batch_runner: %s", exc)
    raise  # Re-raise so Databricks marks the job run as FAILED → Airflow sees failure

# ===========================================================================
# STEP 4 — Return result (notebook context)
# ===========================================================================
dbutils.notebook.exit(final_status)

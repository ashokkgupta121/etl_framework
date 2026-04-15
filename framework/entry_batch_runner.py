# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

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
REPO_ROOT = "/Workspace/Users/ashok.k.gupta121@gmail.com/etl_framework"
MODULES_PATH = f"{REPO_ROOT}/framework/modules"
SQL_PATH = f"{REPO_ROOT}/framework/modules/sql/visionplus"


# DBTITLE 1,Importing Required Libraries
import pandas as pd
import numpy as np


if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if SQL_PATH not in sys.path:
    print(SQL_PATH)
    sys.path.insert(0, SQL_PATH)

# -- Validate path is correct before importing --------------------------------
import importlib, importlib.util
import time
time.sleep(10)

def _assert_module_on_path(module_name: str):
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        print(
            f"Cannot find '{module_name}' on sys.path.\n"
            f"Current sys.path:\n" + "\n".join(sys.path)
        )

for _mod in ["app_config", "session_manager", "secret_manager",
             "audit_manager", "etl_logger", "batch_orchestrator","brz_visionplus_ath2_lrpmt_tgt"]:
    _assert_module_on_path(_mod)

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
spark_temp = sm_temp.get_session()

batch_job_config_id = int(
    SessionManager.resolve_widget_param(spark_temp, "batch_job_config_id", "1")
)
business_date_str = SessionManager.resolve_widget_param(
    spark_temp, "business_date", str(date.today())
)
print(business_date_str)
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
spark       = session_mgr.get_session()

from audit_manager import AuditManager
import threading
batch_log_id = AuditManager.generate_batch_log_id(batch_job_config_id)

session_mgr.set_run_context(spark,
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
    # if final_status == "FAILED":
    #     raise RuntimeError(f"Batch {batch_job_config_id} FAILED for business_date={business_date}.")

except Exception as exc:
    logger.exception("Unhandled exception in entry_batch_runner: %s", exc)
    raise  # Re-raise so Databricks marks the job run as FAILED → Airflow sees failure

# ===========================================================================
# STEP 4 — Return result (notebook context)
# ===========================================================================
#dbutils.notebook.exit(final_status)

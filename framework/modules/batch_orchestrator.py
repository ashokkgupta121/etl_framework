# =============================================================================
# FILE    : framework/modules/batch_orchestrator.py
# MODULE  : 8 — Batch Run Orchestrator
# PURPOSE : Top-level coordinator for a complete batch execution.
#           Resolves config → checks idempotency → groups jobs by execution wave
#           → runs parallel jobs in each wave → logs all outcomes.
#           This is the single entry point called by each Airflow DAG task.
# =============================================================================

from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional
import logging
import traceback

from app_config import JobStatus, FrameworkDefaults
from session_manager import SessionManager
from secret_manager import SecretManager
from audit_manager import AuditManager
from etl_logger import ETLLogger
from transformation_engine import TransformationEngine

logger = logging.getLogger("etl.batch_orchestrator")


class BatchOrchestrator:
    """
    Drives a complete batch run for a given batch_job_config_id + business_date.

    Execution flow:
        1.  Resolve session context (business_date, trigger_type)
        2.  Fetch batch_job_config and all active job_configs
        3.  IDEMPOTENCY CHECK: if batch already COMPLETED for business_date → SKIP
        4.  Insert RUNNING record into batch_job_log
        5.  Group jobs into execution waves (by execution_order)
        6.  For each wave:
              a. Run jobs in parallel (up to max_parallel_jobs)
              b. Each job checks its own idempotency + dependency guard
              c. Log RUNNING → COMPLETED / FAILED / SKIPPED per job
        7.  Update batch_job_log to COMPLETED / FAILED

    Usage (from entry notebook or Airflow task):
        orchestrator = BatchOrchestrator(spark, dbutils)
        orchestrator.run(
            batch_job_config_id = 1,
            business_date       = date(2025, 7, 24),
            airflow_dag_id      = "VP-1_scheduled_2025-07-24T02:00:00",
            trigger_type        = "SCHEDULED",
        )
    """

    def __init__(self, spark: SparkSession, dbutils=None):
        self._spark    = spark
        self._dbutils  = dbutils

        env_config              = SessionManager().env_config
        secret_mgr              = SecretManager(spark, scope=env_config.key_vault_scope)
        self._audit_mgr         = AuditManager(spark)
        self._logger            = ETLLogger(spark)
        self._transform_engine  = TransformationEngine(spark, dbutils)

    # =========================================================================
    # PUBLIC ENTRY POINT
    # =========================================================================

    def run(
        self,
        batch_job_config_id: int,
        business_date:       date,
        airflow_dag_id:      str,
        trigger_type:        str = "SCHEDULED",
    ) -> str:
        """
        Runs the full batch. Returns the final batch_status string.
        Designed to be called from either:
          - An Airflow PythonOperator (imports this class)
          - A Databricks entry notebook (instantiates and calls .run())
        """
        logger.info(
            "=== BATCH START === batch_job_config_id=%s  business_date=%s  dag=%s",
            batch_job_config_id, business_date, airflow_dag_id
        )

        # ── Step 1: generate batch log ID ────────────────────────────────────
        batch_log_id = self._audit_mgr.generate_batch_log_id(batch_job_config_id)

        # ── Step 2: idempotency guard ─────────────────────────────────────────
        if self._audit_mgr.is_batch_already_completed(batch_job_config_id, business_date):
            self._logger.start_batch(
                batch_log_id, batch_job_config_id, business_date, airflow_dag_id, trigger_type
            )
            self._logger.skip_batch(
                batch_log_id,
                f"Batch already COMPLETED for business_date={business_date}."
            )
            return JobStatus.SKIPPED

        # ── Step 3: fetch config ──────────────────────────────────────────────
        batch_cfg   = self._audit_mgr.get_batch_config(batch_job_config_id)
        all_jobs    = self._audit_mgr.get_active_jobs_for_batch(batch_job_config_id)
        max_workers = batch_cfg.get("max_parallel_jobs") or FrameworkDefaults.MAX_PARALLEL_JOBS

        if not all_jobs:
            logger.warning("No active jobs found for batch_job_config_id=%s", batch_job_config_id)
            return JobStatus.SKIPPED

        # ── Step 4: start batch log ───────────────────────────────────────────
        self._logger.start_batch(
            batch_log_id, batch_job_config_id, business_date, airflow_dag_id, trigger_type
        )

        # ── Step 5: group into execution waves ────────────────────────────────
        waves = self._audit_mgr.group_jobs_by_execution_order(all_jobs)
        logger.info("Execution plan: %d wave(s), %d total jobs.", len(waves), len(all_jobs))

        # ── Step 6: execute wave by wave ──────────────────────────────────────
        counters = {"completed": 0, "failed": 0, "skipped": 0}

        try:
            for wave_idx, wave_jobs in enumerate(waves, start=1):
                logger.info(
                    "--- Wave %d / %d  (%d jobs) ---",
                    wave_idx, len(waves), len(wave_jobs)
                )
                self._run_wave(
                    jobs             = wave_jobs,
                    business_date    = business_date,
                    batch_log_id     = batch_log_id,
                    airflow_dag_id   = airflow_dag_id,
                    max_workers      = max_workers,
                    counters         = counters,
                )

        except Exception as exc:
            # Catastrophic orchestrator-level failure
            error_msg = f"Orchestrator error: {traceback.format_exc()}"
            logger.error(error_msg)
            self._logger.fail_batch(batch_log_id, error_msg)
            return JobStatus.FAILED

        # ── Step 7: finalise batch log ────────────────────────────────────────
        self._logger.complete_batch(
            batch_job_log_id = batch_log_id,
            total_jobs       = len(all_jobs),
            jobs_completed   = counters["completed"],
            jobs_failed      = counters["failed"],
            jobs_skipped     = counters["skipped"],
        )

        final_status = JobStatus.FAILED if counters["failed"] > 0 else JobStatus.COMPLETED
        logger.info(
            "=== BATCH END === status=%s  completed=%s  failed=%s  skipped=%s",
            final_status, counters["completed"], counters["failed"], counters["skipped"]
        )
        return final_status

    # =========================================================================
    # WAVE EXECUTION (parallel within a wave)
    # =========================================================================

    def _run_wave(
        self,
        jobs:           list,
        business_date:  date,
        batch_log_id:   str,
        airflow_dag_id: str,
        max_workers:    int,
        counters:       dict,
    ) -> None:
        """
        Executes all jobs in a single wave using a thread pool.
        Each thread handles one table load end-to-end.
        """
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    self._run_single_job,
                    job_row        = job,
                    business_date  = business_date,
                    batch_log_id   = batch_log_id,
                    airflow_dag_id = airflow_dag_id,
                ): job
                for job in jobs
            }

            for future in as_completed(futures):
                job_row = futures[future]
                status  = future.result()   # _run_single_job always returns a status string
                counters[status.lower()] = counters.get(status.lower(), 0) + 1

    # =========================================================================
    # SINGLE JOB EXECUTION
    # =========================================================================

    def _run_single_job(
        self,
        job_row:       object,
        business_date: date,
        batch_log_id:  str,
        airflow_dag_id: str,
    ) -> str:
        """
        Full lifecycle for one table load:
            idempotency check → dependency check → RUNNING → transform → COMPLETE/FAIL/SKIP

        Always returns a JobStatus string (never raises).
        """
        job_config_id = job_row["job_config_id"]
        target_table  = f"{job_row['target_schema']}.{job_row['target_table_name']}"
        notebook_path = job_row.get("notebook_name") or ""
        audit_log_id  = self._audit_mgr.generate_job_audit_log_id()

        # -- Insert RUNNING log record
        self._logger.start_job(
            job_audit_log_id    = audit_log_id,
            job_config_id       = job_config_id,
            batch_job_config_id = job_row["batch_job_config_id"],
            batch_job_log_id    = batch_log_id,
            batch_airflow_dag_id= airflow_dag_id,
            business_date       = business_date,
            notebook_path       = notebook_path,
        )

        # -- Idempotency check
        if self._audit_mgr.is_job_already_completed(job_config_id, business_date):
            self._logger.skip_job(
                audit_log_id,
                f"Already COMPLETED for business_date={business_date}."
            )
            return JobStatus.SKIPPED

        # -- Dependency check
        deps_met, dep_reason = self._audit_mgr.check_dependencies_met(job_row, business_date)
        if not deps_met:
            self._logger.skip_job(audit_log_id, dep_reason)
            return JobStatus.SKIPPED

        # -- Run transformation
        try:
            metrics = self._transform_engine.run_job(
                job_row          = job_row,
                business_date    = business_date,
                batch_job_log_id = batch_log_id,
            )

            # Update watermark for incremental loads
            if job_row["table_strategy"] in ("INC",) and metrics.get("watermark_value_new"):
                self._audit_mgr.update_watermark(job_config_id, metrics["watermark_value_new"])

            self._logger.complete_job(audit_log_id, **metrics)
            logger.info("Job COMPLETED — %s", target_table)
            return JobStatus.COMPLETED

        except Exception as exc:
            error_desc = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
            logger.error("Job FAILED — %s\n%s", target_table, error_desc)
            self._logger.fail_job(audit_log_id, error_desc)
            return JobStatus.FAILED

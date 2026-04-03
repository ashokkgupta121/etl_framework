# =============================================================================
# FILE    : framework/modules/etl_logger.py
# MODULE  : 1 — ETL Logging Module
# PURPOSE : Writes and updates batch_job_log and job_audit_log metadata tables.
#           All status transitions (RUNNING → COMPLETED/FAILED/SKIPPED) go
#           through this module. No other module writes to log tables directly.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, date
from typing import Optional
import logging

from app_config import MetadataTable, JobStatus

logger = logging.getLogger("etl.etl_logger")


class ETLLogger:
    """
    Handles all writes to the two runtime log tables:
        - etl_metadata.batch_job_log     (one row per batch run)
        - etl_metadata.job_audit_log     (one row per table load)

    All upserts use Delta MERGE to guarantee idempotency on reruns.

    Usage:
        etl_log = ETLLogger(spark)

        # Batch lifecycle
        etl_log.start_batch(batch_job_log_id, batch_job_config_id, business_date, dag_id)
        etl_log.complete_batch(batch_job_log_id, stats)
        etl_log.fail_batch(batch_job_log_id, error_message)

        # Table lifecycle
        log_id = etl_log.start_job(...)
        etl_log.complete_job(log_id, row_counts)
        etl_log.fail_job(log_id, error_desc)
        etl_log.skip_job(log_id, skip_reason)
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # =========================================================================
    # BATCH-LEVEL LOGGING
    # =========================================================================

    def start_batch(
        self,
        batch_job_log_id:    str,
        batch_job_config_id: int,
        business_date:       date,
        batch_airflow_dag_id: str,
        trigger_type:        str = "SCHEDULED",
    ) -> None:
        """Inserts a RUNNING record into batch_job_log at the start of a batch."""
        now = datetime.utcnow()
        data = [{
            "batch_job_log_id":     batch_job_log_id,
            "batch_job_config_id":  batch_job_config_id,
            "business_date":        str(business_date),
            "batch_start_date":     now,
            "batch_end_date":       None,
            "batch_status":         JobStatus.RUNNING,
            "batch_airflow_dag_id": batch_airflow_dag_id,
            "triggered_by":         trigger_type,
            "total_jobs":           0,
            "jobs_completed":       0,
            "jobs_failed":          0,
            "jobs_skipped":         0,
            "error_message":        None,
            "created_dt":           now,
            "updated_dt":           now,
        }]
        df = self._spark.createDataFrame(data)
        self._merge_batch_log(df, batch_job_log_id)
        logger.info("Batch STARTED — log_id=%s  business_date=%s", batch_job_log_id, business_date)

    def complete_batch(
        self,
        batch_job_log_id: str,
        total_jobs:       int,
        jobs_completed:   int,
        jobs_failed:      int,
        jobs_skipped:     int,
    ) -> None:
        """Updates batch_job_log to COMPLETED (or FAILED if any jobs failed)."""
        status = JobStatus.FAILED if jobs_failed > 0 else JobStatus.COMPLETED
        self._update_batch_log(
            batch_job_log_id = batch_job_log_id,
            status           = status,
            end_time         = datetime.utcnow(),
            total_jobs       = total_jobs,
            jobs_completed   = jobs_completed,
            jobs_failed      = jobs_failed,
            jobs_skipped     = jobs_skipped,
            error_message    = None,
        )
        logger.info("Batch %s — log_id=%s", status, batch_job_log_id)

    def fail_batch(self, batch_job_log_id: str, error_message: str) -> None:
        """Updates batch_job_log to FAILED with error details."""
        self._update_batch_log(
            batch_job_log_id = batch_job_log_id,
            status           = JobStatus.FAILED,
            end_time         = datetime.utcnow(),
            error_message    = error_message[:4000],  # truncate for column width safety
        )
        logger.error("Batch FAILED — log_id=%s  error=%s", batch_job_log_id, error_message)

    def skip_batch(self, batch_job_log_id: str, skip_reason: str) -> None:
        """Updates batch_job_log to SKIPPED (e.g. already completed for business_date)."""
        self._update_batch_log(
            batch_job_log_id = batch_job_log_id,
            status           = JobStatus.SKIPPED,
            end_time         = datetime.utcnow(),
            error_message    = skip_reason,
        )
        logger.info("Batch SKIPPED — log_id=%s  reason=%s", batch_job_log_id, skip_reason)

    # =========================================================================
    # TABLE/JOB-LEVEL LOGGING
    # =========================================================================

    def start_job(
        self,
        job_audit_log_id:    int,
        job_config_id:       int,
        batch_job_config_id: int,
        batch_job_log_id:    str,
        batch_airflow_dag_id: str,
        business_date:       date,
        notebook_path:       str,
        retry_attempt:       int = 0,
    ) -> int:
        """Inserts a RUNNING record into job_audit_log."""
        now = datetime.utcnow()
        data = [{
            "job_audit_log_id":      job_audit_log_id,
            "business_date":         str(business_date),
            "job_config_id":         job_config_id,
            "batch_job_config_id":   batch_job_config_id,
            "batch_airflow_dag_id":  batch_airflow_dag_id,
            "batch_job_log_id":      batch_job_log_id,
            "job_status":            JobStatus.RUNNING,
            "skip_reason":           None,
            "job_start_date":        now,
            "job_end_date":          None,
            "duration_seconds":      None,
            "source_row_count":      None,
            "target_rows_inserted":  None,
            "target_rows_updated":   None,
            "target_rows_deleted":   None,
            "target_total_row_count":None,
            "watermark_value_used":  None,
            "watermark_value_new":   None,
            "error_desc":            None,
            "notebook_path":         notebook_path,
            "retry_attempt":         retry_attempt,
            "created_dt":            now,
            "updated_dt":            now,
        }]
        df = self._spark.createDataFrame(data)
        self._merge_job_log(df, job_audit_log_id)
        logger.info("Job STARTED — audit_id=%s  job_config_id=%s", job_audit_log_id, job_config_id)
        return job_audit_log_id

    def complete_job(
        self,
        job_audit_log_id:       int,
        source_row_count:       int  = 0,
        target_rows_inserted:   int  = 0,
        target_rows_updated:    int  = 0,
        target_rows_deleted:    int  = 0,
        target_total_row_count: int  = 0,
        watermark_value_new:    Optional[str] = None,
    ) -> None:
        """Updates job_audit_log to COMPLETED with row count metrics."""
        end_time = datetime.utcnow()
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.JOB_AUDIT_LOG} AS tgt
            USING (
                SELECT
                    {job_audit_log_id}            AS job_audit_log_id,
                    '{JobStatus.COMPLETED}'        AS job_status,
                    current_timestamp()            AS job_end_date,
                    {source_row_count}             AS source_row_count,
                    {target_rows_inserted}         AS target_rows_inserted,
                    {target_rows_updated}          AS target_rows_updated,
                    {target_rows_deleted}          AS target_rows_deleted,
                    {target_total_row_count}       AS target_total_row_count,
                    {'NULL' if not watermark_value_new else f"'{watermark_value_new}'"} AS watermark_value_new,
                    'Successful Run'               AS error_desc,
                    current_timestamp()            AS updated_dt
            ) AS src ON tgt.job_audit_log_id = src.job_audit_log_id
            WHEN MATCHED THEN UPDATE SET *
        """)
        logger.info("Job COMPLETED — audit_id=%s  inserted=%s  updated=%s",
                    job_audit_log_id, target_rows_inserted, target_rows_updated)

    def fail_job(self, job_audit_log_id: int, error_desc: str) -> None:
        """Updates job_audit_log to FAILED."""
        safe_err = error_desc.replace("'", "''")[:4000]
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.JOB_AUDIT_LOG} AS tgt
            USING (
                SELECT
                    {job_audit_log_id}  AS job_audit_log_id,
                    'FAILED'            AS job_status,
                    current_timestamp() AS job_end_date,
                    '{safe_err}'        AS error_desc,
                    current_timestamp() AS updated_dt
            ) AS src ON tgt.job_audit_log_id = src.job_audit_log_id
            WHEN MATCHED THEN UPDATE SET
                tgt.job_status   = src.job_status,
                tgt.job_end_date = src.job_end_date,
                tgt.error_desc   = src.error_desc,
                tgt.updated_dt   = src.updated_dt
        """)
        logger.error("Job FAILED — audit_id=%s", job_audit_log_id)

    def skip_job(self, job_audit_log_id: int, skip_reason: str) -> None:
        """Updates job_audit_log to SKIPPED."""
        safe_reason = skip_reason.replace("'", "''")
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.JOB_AUDIT_LOG} AS tgt
            USING (
                SELECT
                    {job_audit_log_id}  AS job_audit_log_id,
                    'SKIPPED'           AS job_status,
                    '{safe_reason}'     AS skip_reason,
                    current_timestamp() AS job_end_date,
                    current_timestamp() AS updated_dt
            ) AS src ON tgt.job_audit_log_id = src.job_audit_log_id
            WHEN MATCHED THEN UPDATE SET
                tgt.job_status   = src.job_status,
                tgt.skip_reason  = src.skip_reason,
                tgt.job_end_date = src.job_end_date,
                tgt.updated_dt   = src.updated_dt
        """)
        logger.info("Job SKIPPED — audit_id=%s  reason=%s", job_audit_log_id, skip_reason)

    # =========================================================================
    # Internal MERGE helpers
    # =========================================================================

    def _merge_batch_log(self, df, batch_job_log_id: str) -> None:
        df.createOrReplaceTempView("_batch_log_stage")
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.BATCH_JOB_LOG} AS tgt
            USING _batch_log_stage AS src
              ON tgt.batch_job_log_id = src.batch_job_log_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    def _merge_job_log(self, df, job_audit_log_id: int) -> None:
        df.createOrReplaceTempView("_job_log_stage")
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.JOB_AUDIT_LOG} AS tgt
            USING _job_log_stage AS src
              ON tgt.job_audit_log_id = src.job_audit_log_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    def _update_batch_log(
        self,
        batch_job_log_id: str,
        status:           str,
        end_time:         datetime,
        error_message:    Optional[str] = None,
        total_jobs:       int = 0,
        jobs_completed:   int = 0,
        jobs_failed:      int = 0,
        jobs_skipped:     int = 0,
    ) -> None:
        safe_err = (error_message or "").replace("'", "''")
        self._spark.sql(f"""
            MERGE INTO {MetadataTable.BATCH_JOB_LOG} AS tgt
            USING (
                SELECT
                    '{batch_job_log_id}' AS batch_job_log_id,
                    '{status}'           AS batch_status,
                    '{end_time}'         AS batch_end_date,
                    {total_jobs}         AS total_jobs,
                    {jobs_completed}     AS jobs_completed,
                    {jobs_failed}        AS jobs_failed,
                    {jobs_skipped}       AS jobs_skipped,
                    '{safe_err}'         AS error_message,
                    current_timestamp()  AS updated_dt
            ) AS src ON tgt.batch_job_log_id = src.batch_job_log_id
            WHEN MATCHED THEN UPDATE SET
                tgt.batch_status   = src.batch_status,
                tgt.batch_end_date = src.batch_end_date,
                tgt.total_jobs     = src.total_jobs,
                tgt.jobs_completed = src.jobs_completed,
                tgt.jobs_failed    = src.jobs_failed,
                tgt.jobs_skipped   = src.jobs_skipped,
                tgt.error_message  = src.error_message,
                tgt.updated_dt     = src.updated_dt
        """)

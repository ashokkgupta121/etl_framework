# =============================================================================
# FILE    : framework/modules/audit_manager.py
# MODULE  : 4 — Batch Audit & Idempotency / Skip-Check Logic
# PURPOSE : Reads config metadata, generates run IDs, performs idempotency
#           checks (already-completed guard), and manages watermark state.
#           All "should this run?" decisions live here.
# =============================================================================

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from datetime import datetime, date
from typing import Optional, List, Tuple
import logging

from app_config import MetadataTable, JobStatus

logger = logging.getLogger("etl.audit_manager")


class AuditManager:
    """
    Handles:
      1. Run-ID generation for batch and job log records
      2. Idempotency checks — prevents re-loading already-completed runs
      3. Watermark read/write for incremental loads
      4. Config fetching helpers (batch config, job config list)

    Usage:
        am = AuditManager(spark)

        # Generate IDs
        batch_log_id = am.generate_batch_log_id(1)        # "1_20250724090000"
        job_log_id   = am.generate_job_audit_log_id()      # timestamp-based int

        # Idempotency
        if am.is_batch_already_completed(1, business_date):
            # skip entire batch
            ...

        if am.is_job_already_completed(5, business_date):
            # skip this individual table
            ...

        # Watermark
        last_wm = am.get_watermark(job_config_id=5)
        am.update_watermark(job_config_id=5, new_value="2025-07-24 23:59:59")
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # =========================================================================
    # RUN-ID GENERATION
    # =========================================================================

    @staticmethod
    def generate_batch_log_id(batch_job_config_id: int) -> str:
        """
        Generates a unique batch_job_log_id.
        Format: {batch_job_config_id}_{YYYYMMDDHHmmss}
        Matches the pattern seen in the xlsx: '1_202508020846'
        """
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return f"{batch_job_config_id}_{ts}"

    @staticmethod
    def generate_job_audit_log_id(job_config_id: int) -> str:
        """
        Generates a unique job_audit_log_id as a 14-digit int timestamp.
        Format: YYYYMMDDHHmmssSSS  (matches xlsx pattern: 20250802091944)
        """
        ts = str(job_config_id) +datetime.utcnow().strftime("%Y%m%d%H%M%S")[:14]
        return str(ts)

    # =========================================================================
    # IDEMPOTENCY CHECKS
    # =========================================================================

    def is_batch_already_completed(
        self,
        batch_job_config_id: int,
        business_date:       date,
    ) -> bool:
        """
        Returns True if a COMPLETED batch_job_log record already exists
        for (batch_job_config_id, business_date).

        Called before starting a batch — if True, the orchestrator skips
        all jobs and logs the batch as SKIPPED.
        """
        result = self._spark.sql(f"""
            SELECT COUNT(1) AS cnt
            FROM {MetadataTable.BATCH_JOB_LOG}
            WHERE batch_job_config_id = {batch_job_config_id}
              AND business_date       = '{business_date}'
              AND batch_status        = '{JobStatus.COMPLETED}'
        """).collect()[0]["cnt"]

        if result > 0:
            logger.info(
                "Batch already COMPLETED for batch_job_config_id=%s business_date=%s — will SKIP.",
                batch_job_config_id, business_date
            )
            return True
        return False

    def is_job_already_completed(
        self,
        job_config_id: int,
        business_date: date,
    ) -> bool:
        """
        Returns True if a COMPLETED job_audit_log record already exists
        for (job_config_id, business_date).

        Called before loading each individual table.
        """
        result = self._spark.sql(f"""
            SELECT COUNT(1) AS cnt
            FROM {MetadataTable.JOB_AUDIT_LOG}
            WHERE job_config_id = {job_config_id}
              AND business_date = '{business_date}'
              AND job_status    = '{JobStatus.COMPLETED}'
        """).collect()[0]["cnt"]

        if result > 0:
            logger.info(
                "Job already COMPLETED for job_config_id=%s business_date=%s — will SKIP.",
                job_config_id, business_date
            )
            return True
        return False

    # =========================================================================
    # WATERMARK MANAGEMENT  (for INC / incremental loads)
    # =========================================================================

    def get_watermark(self, job_config_id: int) -> Optional[str]:
        """
        Returns the last successfully written watermark value for a job config.
        Reads source_watermark_value from job_config.
        Returns None if never loaded (triggers full initial load).
        """
        rows = self._spark.sql(f"""
            SELECT source_watermark_value
            FROM {MetadataTable.JOB_CONFIG}
            WHERE job_config_id = {job_config_id}
        """).collect()

        if not rows or rows[0]["source_watermark_value"] is None:
            logger.info("No watermark found for job_config_id=%s — initial full load.", job_config_id)
            return None

        wm = rows[0]["source_watermark_value"]
        logger.info("Watermark for job_config_id=%s → %s", job_config_id, wm)
        return wm

    def update_watermark(self, job_config_id: int, new_value: str) -> None:
        """
        Persists the new high-watermark back into job_config after a
        successful incremental load.
        """
        safe_val = new_value.replace("'", "''")
        self._spark.sql(f"""
            UPDATE {MetadataTable.JOB_CONFIG}
            SET    source_watermark_value = '{safe_val}',
                   updated_dt             = current_timestamp()
            WHERE  job_config_id          = {job_config_id}
        """)
        logger.info("Watermark updated — job_config_id=%s  new_value=%s", job_config_id, new_value)

    # =========================================================================
    # CONFIG FETCHERS
    # =========================================================================

    def get_batch_config(self, batch_job_config_id: int) -> Row:
        """Returns the batch_job_config row for a given batch ID."""
        rows = self._spark.sql(f"""
            SELECT * FROM {MetadataTable.BATCH_JOB_CONFIG}
            WHERE batch_job_config_id = {batch_job_config_id}
              AND is_active = TRUE
        """).collect()

        if not rows:
            raise ValueError(
                f"No active batch_job_config found for id={batch_job_config_id}."
            )
        return rows[0]

    def get_active_jobs_for_batch(self, batch_job_config_id: int) -> List[Row]:
        """
        Returns all active job_config rows for a batch, ordered by execution_order.
        Jobs with the same execution_order are candidates for parallel execution.
        """
        return self._spark.sql(f"""
            SELECT *
            FROM {MetadataTable.JOB_CONFIG}
            WHERE batch_job_config_id = {batch_job_config_id}
              AND is_active           = TRUE
            ORDER BY execution_order ASC, job_config_id ASC
        """).collect()

    def get_job_config(self, job_config_id: int) -> Row:
        """Returns a single job_config row."""
        rows = self._spark.sql(f"""
            SELECT * FROM {MetadataTable.JOB_CONFIG}
            WHERE job_config_id = {job_config_id}
        """).collect()
        if not rows:
            raise ValueError(f"No job_config found for id={job_config_id}.")
        return rows[0]

    def get_connection_config(self, connection_id: str) -> Optional[Row]:
        """Returns the connection config row for a connection_id."""
        if not connection_id:
            return None
        rows = self._spark.sql(f"""
            SELECT * FROM {MetadataTable.CONNECTION_CONFIG}
            WHERE connection_id = '{connection_id}'
              AND is_active     = TRUE
        """).collect()
        return rows[0] if rows else None

    # =========================================================================
    # DEPENDENCY RESOLUTION
    # =========================================================================

    def group_jobs_by_execution_order(
        self,
        jobs: List[Row]
    ) -> List[List[Row]]:
        """
        Groups job_config rows into execution waves.
        Jobs in the same wave (same execution_order) run in parallel.
        Waves are returned in ascending order.

        Example:
            Wave 1 (order=1): [job_A, job_B, job_C]  ← parallel
            Wave 2 (order=2): [job_D]                 ← sequential (depends on wave 1)
        """
        from collections import defaultdict
        waves: dict = defaultdict(list)
        for job in jobs:
            waves[job["execution_order"]].append(job)
        return [waves[k] for k in sorted(waves.keys())]

    def check_dependencies_met(
        self,
        job_row:       Row,
        business_date: date,
    ) -> Tuple[bool, str]:
        """
        Checks if all jobs listed in depends_on_job_ids have COMPLETED
        for the given business_date.

        Returns (True, "") if all deps met, or (False, reason_string) if not.
        """
        depends_on_raw = job_row.depends_on_job_ids or ""
        if not depends_on_raw.strip():
            return True, ""

        dep_ids = [d.strip() for d in depends_on_raw.split(",") if d.strip()]
        unmet = []

        for dep_id in dep_ids:
            completed = self.is_job_already_completed(int(dep_id), business_date)
            if not completed:
                unmet.append(dep_id)

        if unmet:
            reason = f"Dependency jobs not yet completed: {unmet}"
            logger.warning(reason)
            return False, reason
        print("dependency check completed")
        return True, ""

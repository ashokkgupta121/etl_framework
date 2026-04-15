# =============================================================================
# FILE    : framework/modules/etl_helpers.py
# MODULE  : 5 — Helper Utilities
# PURPOSE : Reusable helper functions for Delta writes, schema enforcement,
#           audit column injection, DQ row count checks, and retry logic.
#           These functions are stateless — they do not call log tables.
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from datetime import datetime, date
from typing import Optional, List, Callable, Any
import time
import logging

from app_config import LoadStrategy, FrameworkDefaults

logger = logging.getLogger("etl.etl_helpers")


# =============================================================================
# DELTA WRITE HELPERS
# =============================================================================

class DeltaWriter:
    """
    Handles all target Delta table writes with support for all load strategies.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def write(
        self,
        df:                 DataFrame,
        target_table:       str,
        load_strategy:      str,
        primary_key_cols:   Optional[str]  = None,
        partition_cols:     Optional[str]  = None,
        schema_evolution:   bool           = True,
    ) -> dict:
        """
        Routes to the correct write method based on load_strategy.
        Returns row-count metrics dict.
        """
        pk_list   = [c.strip() for c in (primary_key_cols or "").split(",") if c.strip()]
        part_list = [c.strip() for c in (partition_cols   or "").split(",") if c.strip()]
        strategy  = load_strategy.upper()

        if strategy == LoadStrategy.FULL:
            return self._write_full(df, target_table, part_list, schema_evolution)
        elif strategy in (LoadStrategy.INC, LoadStrategy.SCD1):
            return self._write_merge(df, target_table, pk_list, schema_evolution)
        elif strategy == LoadStrategy.SCD2:
            return self._write_scd2(df, target_table, pk_list)
        elif strategy == LoadStrategy.APPEND:
            return self._write_append(df, target_table, part_list, schema_evolution)
        else:
            raise ValueError(f"Unsupported load_strategy: '{strategy}'")

    # ------------------------------------------------------------------
    def _write_full(
        self,
        df:             DataFrame,
        target_table:   str,
        partition_cols: List[str],
        schema_evolution: bool,
    ) -> dict:
        """Overwrites the entire target table (FULL load)."""
        print("before source count")
        source_count = df.count()
        print(f"target table is {target_table}")
        writer = df.write.format("delta").mode("overwrite")
        if schema_evolution:
            writer = writer.option("overwriteSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(target_table,inferSchema=True)
        
        return {
            "source_row_count":       source_count,
            "target_rows_inserted":   source_count,
            "target_rows_updated":    0,
            "target_rows_deleted":    0,
            "target_total_row_count": source_count,
        }

    # ------------------------------------------------------------------
    def _write_append(
        self,
        df:             DataFrame,
        target_table:   str,
        partition_cols: List[str],
        schema_evolution: bool,
    ) -> dict:
        """Appends records to the target table."""
        source_count = df.count()
        writer = df.write.format("delta").mode("append")
        if schema_evolution:
            writer = writer.option("mergeSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(target_table)

        total = self._spark.sql(f"SELECT COUNT(1) AS cnt FROM {target_table}").collect()[0]["cnt"]
        return {
            "source_row_count":       source_count,
            "target_rows_inserted":   source_count,
            "target_rows_updated":    0,
            "target_rows_deleted":    0,
            "target_total_row_count": total,
        }

    # ------------------------------------------------------------------
    def _write_merge(
        self,
        df:             DataFrame,
        target_table:   str,
        pk_cols:        List[str],
        schema_evolution: bool,
    ) -> dict:
        """
        Performs a Delta MERGE (upsert) for INC / SCD1 strategy.
        Inserts new rows, updates existing rows matched on PKs.
        """
        if not pk_cols:
            raise ValueError(f"primary_key_columns required for MERGE strategy on {target_table}.")

        source_count = df.count()
        join_cond = " AND ".join(
            [f"{FrameworkDefaults.MERGE_TARGET_ALIAS}.{c} = {FrameworkDefaults.MERGE_SOURCE_ALIAS}.{c}"
             for c in pk_cols]
        )
        update_set = ", ".join(
            [f"{FrameworkDefaults.MERGE_TARGET_ALIAS}.{c} = {FrameworkDefaults.MERGE_SOURCE_ALIAS}.{c}"
             for c in df.columns]
        )

        df.createOrReplaceTempView("_merge_source")

        if schema_evolution:
            self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        result = self._spark.sql(f"""
            MERGE INTO {target_table} AS {FrameworkDefaults.MERGE_TARGET_ALIAS}
            USING _merge_source      AS {FrameworkDefaults.MERGE_SOURCE_ALIAS}
            ON {join_cond}
            WHEN MATCHED     THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Databricks MERGE returns operation metrics
        metrics = self._get_merge_metrics(target_table)
        return {
            "source_row_count":       source_count,
            "target_rows_inserted":   metrics.get("numTargetRowsInserted", 0),
            "target_rows_updated":    metrics.get("numTargetRowsUpdated",  0),
            "target_rows_deleted":    0,
            "target_total_row_count": metrics.get("numTargetRowsAfterMerge", 0),
        }

    # ------------------------------------------------------------------
    def _write_scd2(
        self,
        df:      DataFrame,
        target_table: str,
        pk_cols: List[str],
    ) -> dict:
        """
        SCD Type 2: expire old rows (set is_current=False, eff_end_dt=today),
        insert new rows with is_current=True and new eff_start_dt.
        Expects source DataFrame to already contain change records only.
        """
        if not pk_cols:
            raise ValueError(f"primary_key_columns required for SCD2 on {target_table}.")

        today    = datetime.utcnow().isoformat()
        src_count = df.count()

        join_cond    = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])
        update_set   = f"tgt.is_current = FALSE, tgt.eff_end_dt = '{today}', tgt.updated_dt = current_timestamp()"

        df.createOrReplaceTempView("_scd2_source")
        self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        self._spark.sql(f"""
            MERGE INTO {target_table} AS tgt
            USING _scd2_source        AS src
            ON {join_cond} AND tgt.is_current = TRUE
            WHEN MATCHED AND (
                md5(concat_ws('||', tgt.*)) <> md5(concat_ws('||', src.*))
            ) THEN UPDATE SET {update_set}
        """)

        # Insert new active rows
        (df
         .withColumn("is_current",  F.lit(True))
         .withColumn("eff_start_dt", F.lit(today))
         .withColumn("eff_end_dt",   F.lit(None).cast("string"))
         .write.format("delta").mode("append").saveAsTable(target_table)
        )

        total = self._spark.sql(f"SELECT COUNT(1) AS cnt FROM {target_table}").collect()[0]["cnt"]
        return {
            "source_row_count":       src_count,
            "target_rows_inserted":   src_count,
            "target_rows_updated":    src_count,  # approximation
            "target_rows_deleted":    0,
            "target_total_row_count": total,
        }

    # ------------------------------------------------------------------
    def _get_merge_metrics(self, target_table: str) -> dict:
        try:
            hist = self._spark.sql(
                f"DESCRIBE HISTORY {target_table} LIMIT 1"
            ).collect()[0]
            return hist.get("operationMetrics") or {}
        except Exception:
            return {}


# =============================================================================
# AUDIT COLUMN INJECTOR
# =============================================================================

def add_audit_columns(
    df:                 DataFrame,
    batch_job_log_id:   str,
    job_config_id:      int,
    business_date:      date,
    load_strategy:      str,
) -> DataFrame:
    """
    Appends standard ETL audit columns to a DataFrame before writing to target.
    These columns are present on every table in every layer.
    """
    return (
        df
        .withColumn("etl_batch_job_log_id", F.lit(batch_job_log_id))
        .withColumn("etl_job_config_id",    F.lit(job_config_id))
        .withColumn("etl_business_date",    F.lit(str(business_date)).cast("date"))
        .withColumn("etl_load_ts",          F.current_timestamp())
        .withColumn("etl_load_strategy",    F.lit(load_strategy))
    )


# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

def check_min_row_count(
    spark:                  SparkSession,
    target_table:           str,
    expected_min_row_count: Optional[int],
) -> bool:
    """
    Returns False (DQ failure) if target table has fewer rows than the minimum.
    Skips check if expected_min_row_count is None.
    """
    if not expected_min_row_count:
        return True

    actual = spark.sql(f"SELECT COUNT(1) AS cnt FROM {target_table}").collect()[0]["cnt"]
    if actual < expected_min_row_count:
        logger.warning(
            "DQ FAIL: %s has %s rows, expected minimum %s.",
            target_table, actual, expected_min_row_count
        )
        return False
    return True


# =============================================================================
# RETRY DECORATOR
# =============================================================================

def with_retry(
    func:           Callable,
    max_attempts:   int = FrameworkDefaults.MAX_RETRY_ATTEMPTS,
    delay_seconds:  int = FrameworkDefaults.RETRY_DELAY_SECONDS,
    reraise:        bool = True,
) -> Any:
    """
    Calls `func()` with automatic retry on exception.
    Returns the result of the first successful call.
    Raises the last exception if all attempts are exhausted and reraise=True.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 2):  # +2 = initial + retries
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if attempt <= max_attempts:
                logger.warning(
                    "Attempt %s failed: %s — retrying in %ss...",
                    attempt, exc, delay_seconds
                )
                time.sleep(delay_seconds)
            else:
                logger.error("All %s attempts exhausted. Last error: %s", attempt, exc)

    if reraise and last_exc:
        raise last_exc


# =============================================================================
# SCHEMA UTILITIES
# =============================================================================

def safe_cast_schema(df: DataFrame, target_schema: StructType) -> DataFrame:
    """
    Casts DataFrame columns to match target_schema types.
    Adds missing columns as NULL. Ignores extra columns.
    Useful for schema enforcement at Silver/Gold write.
    """
    result = df
    existing = {f.name.lower(): f.name for f in df.schema}

    for field in target_schema:
        col_lower = field.name.lower()
        if col_lower in existing:
            result = result.withColumn(field.name, F.col(existing[col_lower]).cast(field.dataType))
        else:
            result = result.withColumn(field.name, F.lit(None).cast(field.dataType))
            logger.debug("Added missing column: %s", field.name)

    return result.select([f.name for f in target_schema])

# =============================================================================
# FILE    : framework/modules/transformation_engine.py
# MODULE  : 9 — Table Transformation Engine
# PURPOSE : Executes SQL transformation notebooks for a given job_config row.
#           Resolves the correct SQL notebook path, runs each query in order,
#           applies audit columns, and writes to the target Delta table.
#           Supports multi-query notebooks (query_001, query_002 … per table).
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import date
from typing import Optional, List
import logging

from app_config import LoadStrategy, FrameworkDefaults, Layer, resolve_notebook_path
from etl_helpers import DeltaWriter, add_audit_columns, check_min_row_count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from brz_visionplus_ath2_lrpmt_tgt import BrzVisionplusAth2LrpmtTgt
from brz_visionplus_ath3d_tgt import   BrzVisionplusAth3dTgt
from brz_visionplus_ath3x_tgt import BrzVisionplusAth3xTgt
from brz_visionplus_atptrpt_tgt import BrzVisionplusAtptrptTgt



logger = logging.getLogger("etl.transformation_engine")


class TransformationEngine:
    """
    Orchestrates the transformation and load of a single table.

    Execution flow per job_config row:
        1. Resolve SQL notebook path (from metadata or convention)
        2. Run pre-hook notebook (optional)
        3. Execute transformation SQL queries in order (query_001, query_002 …)
        4. Apply ETL audit columns
        5. Write to target Delta table using the configured load_strategy
        6. Run DQ row-count check (optional)
        7. Run post-hook notebook (optional)
        8. Return row-count metrics

    Usage:
        engine = TransformationEngine(spark, dbutils)
        metrics = engine.run_job(
            job_row         = job_config_row,
            business_date   = date(2025, 7, 24),
            batch_job_log_id = "1_20250724090000",
        )
    """
    

    def get_empty_audit_df(self):
        empty_df = self._spark.range(10) 
        return empty_df

# Usage


    def __init__(self, spark: SparkSession, dbutils=None):
        self._spark    = spark
        self._dbutils  = dbutils
        self._writer   = DeltaWriter(spark)

    # =========================================================================
    # PUBLIC ENTRY POINT
    # =========================================================================

    def run_job(
        self,
        job_row:          object,   # Row from job_config
        business_date:    date,
        batch_job_log_id: str,
    ) -> dict:
        """
        Executes the full transformation pipeline for a single table.
        Returns a metrics dict compatible with ETLLogger.complete_job().
        """
        job_config_id     = job_row["job_config_id"]
        target_table      = f"{job_row['target_schema']}.{job_row['target_table_name']}"
        load_strategy     = job_row["table_strategy"]
        notebook_name     = job_row["notebook_name"]
        pk_cols           = job_row["primary_key_columns"] if job_row["primary_key_columns"] is not None else ""
        partition_cols    = job_row["partition_columns"] or ""
        expected_min_rows = job_row["expected_min_row_count"]

        logger.info(
            "TransformationEngine.run_job — job_config_id=%s  target=%s  strategy=%s",
            job_config_id, target_table, load_strategy
        )

        # -- Pre-hook (optional)
        #self._run_hook(job_row.pre_hook_notebook, business_date, batch_job_log_id)

        # -- Execute transformation SQL, build final DataFrame
        df = self._execute_transformation_notebook(
            notebook_name    = notebook_name,
            business_date    = business_date,
            batch_job_log_id = batch_job_log_id,
            job_row          = job_row,
        )
        

        # -- Inject audit columns
        df = add_audit_columns(
            df               = df,
            batch_job_log_id = batch_job_log_id,
            job_config_id    = job_config_id,
            business_date    = business_date,
            load_strategy    = load_strategy,
        )
        

        # -- Write to Delta target
        metrics = self._writer.write(
            df               = df,
            target_table     = target_table,
            load_strategy    = load_strategy,
            primary_key_cols = pk_cols,
            partition_cols   = partition_cols,
        )
        

        # -- DQ check
        dq_passed = check_min_row_count(self._spark, target_table, expected_min_rows)
        if not dq_passed:
            raise ValueError(
                f"DQ row-count check FAILED for {target_table}. "
                f"Expected minimum: {expected_min_rows}, "
                f"Actual: {metrics['target_total_row_count']}"
            )

        # -- Post-hook (optional)
        #self._run_hook(job_row.post_hook_notebook, business_date, batch_job_log_id)

        return metrics

    # =========================================================================
    # TRANSFORMATION NOTEBOOK EXECUTOR
    # =========================================================================

    def _execute_transformation_notebook(
        self,
        notebook_name:    str,
        business_date:    date,
        batch_job_log_id: str,
        job_row:          object,
    ) -> DataFrame:
        """
        Resolves the full notebook path and runs the transformation.

        SQL notebooks contain one or more named queries following the convention:
            query_001_extract   – source data read (returns a temp view)
            query_002_transform – transformations on top of extract
            query_003_final     – final SELECT producing the target DataFrame

        The last query in the notebook must produce the final target DataFrame.
        Each query is registered as a temp view named after its step key.

        For Databricks, notebooks are run via dbutils.notebook.run() with
        parameters; the last cell must call dbutils.notebook.exit(result).
        For pure PySpark (non-notebook) mode, queries are run as spark.sql().
        """
        notebook_path = self._resolve_notebook_path(notebook_name, job_row)

        logger.info("Running transformation notebook: %s", notebook_path)

        # Parameters passed to every transformation notebook
        params = {
            "business_date":       str(business_date),
            "batch_job_log_id":    batch_job_log_id,
            "job_config_id":       str(job_row["job_config_id"]),
            "target_schema":       job_row["target_schema"],
            "target_table_name":   job_row["target_table_name"],
            "table_strategy":      job_row["table_strategy"],
            "watermark_value":     str(job_row.source_watermark_value or
                                       FrameworkDefaults.DEFAULT_WATERMARK_DATE),
            "notebook_name":     str(job_row.notebook_name),
            
        }

        # -- Databricks notebook execution path
        if self._dbutils:
            result_view = self._run_databricks_notebook(notebook_path, params)
            print(f"after run_databricks_notebook{result_view}")
            #return self._spark.table(result_view)
            return result_view

        # -- Fallback: direct SQL execution (for testing / non-notebook mode)
        #return self._run_sql_queries_directly(notebook_path, params)

    def _run_databricks_notebook(self, notebook_path: str, params: dict) -> str:
        """
        Runs a Databricks notebook via dbutils.notebook.run().
        The notebook must exit with the name of the temp view containing results.
        """
        timeout_seconds = 3600  # 1 hour max per table
        result_view = None
        

        class_obj = globals()[params.get('notebook_name')]({'watermark_value': '2026-03-01', 'business_date': '2026-02-01'})
        
        queries = getattr(class_obj, "queries", None)

        

        for query in queries:
            df = self._spark.sql(query["sql"])
            df.createOrReplaceTempView(query["name"])
        result_view = df
        #result_view = self.get_empty_audit_df()

        print(f"after result_view")
        if not result_view:
            raise RuntimeError(
                f"Notebook {notebook_path} did not return a result view name. "
                "Ensure the notebook ends with: dbutils.notebook.exit('<view_name>')"
            )
        return result_view

   

    # =========================================================================
    # HELPERS
    # =========================================================================

    @staticmethod
    def _resolve_notebook_path(notebook_name: str, job_row: object) -> str:
        """
        If notebook_name is a full path (starts with '/'), use as-is.
        Otherwise treat as relative path under framework/sql/.
        """
        if notebook_name.startswith("/") or notebook_name.startswith("framework/"):
            return notebook_name
        return f"sql/{notebook_name}"

    @staticmethod
    def _parse_sql_queries(raw_sql: str) -> dict:
        """
        Parses a multi-query SQL file into an ordered dict of {step_name: sql}.

        Convention: queries are separated by comment markers:
            -- query_001_step_name
            SELECT ...;
            -- query_002_another_step
            SELECT ...;
        """
        import re
        pattern = r"--\s*(query_\d{3}_\w+)\s*\n"
        parts   = re.split(pattern, raw_sql.strip())
        queries = {}

        for i in range(1, len(parts), 2):
            step_name  = parts[i].strip()
            query_body = parts[i + 1].strip().rstrip(";")
            if query_body:
                queries[step_name] = query_body

        # If no markers found, treat entire file as a single query
        if not queries:
            queries["query_001_main"] = raw_sql.strip().rstrip(";")

        return queries

    @staticmethod
    def _interpolate_params(sql: str, params: dict) -> str:
        """
        Replaces ${param_name} placeholders in SQL with actual values.
        """
        for k, v in params.items():
            sql = sql.replace(f"${{{k}}}", v)
        return sql

    def _run_hook(
        self,
        hook_path:        Optional[str],
        business_date:    date,
        batch_job_log_id: str,
    ) -> None:
        """Runs a pre- or post-hook notebook if configured."""
        if not hook_path or not self._dbutils:
            return
        logger.info("Running hook notebook: %s", hook_path)
        self._dbutils.notebook.run(hook_path, 1800, {
            "business_date":    str(business_date),
            "batch_job_log_id": batch_job_log_id,
        })

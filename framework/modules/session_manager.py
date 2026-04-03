# =============================================================================
# FILE    : framework/modules/session_manager.py
# MODULE  : 2 — Spark Session & Session-Level Variables
# PURPOSE : Creates/retrieves the SparkSession with framework-standard configs.
#           Sets session-level Spark SQL variables accessible across notebooks.
#           Also resolves runtime parameters (business_date, batch_id, etc.)
#           from Databricks widgets or Airflow-passed job parameters.
# =============================================================================

from pyspark.sql import SparkSession
from datetime import date, datetime
from typing import Optional
import logging

from app_config import EnvironmentConfig, FrameworkDefaults

logger = logging.getLogger("etl.session_manager")


# =============================================================================
# SESSION-LEVEL VARIABLE KEYS — avoids magic strings across modules
# =============================================================================

class SessionVar:
    BUSINESS_DATE       = "etl.business_date"
    BATCH_JOB_CONFIG_ID = "etl.batch_job_config_id"
    BATCH_JOB_LOG_ID    = "etl.batch_job_log_id"
    TRIGGER_TYPE        = "etl.trigger_type"
    ENV                 = "etl.env"
    CURRENT_USER        = "etl.current_user"
    LOAD_TS             = "etl.load_ts"


# =============================================================================
# SPARK SESSION MANAGER
# =============================================================================

class SessionManager:
    """
    Manages the SparkSession lifecycle and session-level variables for the ETL framework.

    Usage (in orchestrator or entry notebook):
        sm = SessionManager(env_config)
        spark = sm.get_or_create_session()
        sm.set_run_context(
            business_date       = date(2025, 7, 24),
            batch_job_config_id = 1,
            batch_job_log_id    = "1_20250724090000",
            trigger_type        = "SCHEDULED"
        )
    """

    def __init__(self, env_config: Optional[EnvironmentConfig] = None):
        self.env_config = env_config or EnvironmentConfig()
        self._spark: Optional[SparkSession] = None

    # ------------------------------------------------------------------
    # Session creation
    # ------------------------------------------------------------------

    def get_or_create_session(self) -> SparkSession:
        """
        Returns the active SparkSession (Databricks always has one active).
        Applies framework-standard configs on top.
        """
        self._spark = (
            SparkSession.builder
            .appName("ETL_Framework")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.optimizeWrite.enabled",    "true")
            .config("spark.databricks.delta.autoCompact.enabled",      "true")
            .config("spark.sql.shuffle.partitions",                    "200")
            .config("spark.sql.adaptive.enabled",                      "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled",   "true")
            # Ensure consistent NULL handling
            .config("spark.sql.ansi.enabled",                          "true")
            .getOrCreate()
        )
        logger.info("SparkSession initialised — app: %s", self._spark.sparkContext.appName)
        return self._spark

    # ------------------------------------------------------------------
    # Run-context setter (called once per batch run by the orchestrator)
    # ------------------------------------------------------------------

    def set_run_context(
        self,
        business_date:       date,
        batch_job_config_id: int,
        batch_job_log_id:    str,
        trigger_type:        str = "SCHEDULED",
    ) -> None:
        """
        Persists run-level variables into the active Spark session so they are
        accessible via spark.conf.get(SessionVar.XXX) in any downstream notebook.
        """
        if self._spark is None:
            raise RuntimeError("Call get_or_create_session() before set_run_context().")

        ctx = {
            SessionVar.BUSINESS_DATE:       str(business_date),
            SessionVar.BATCH_JOB_CONFIG_ID: str(batch_job_config_id),
            SessionVar.BATCH_JOB_LOG_ID:    str(batch_job_log_id),
            SessionVar.TRIGGER_TYPE:        trigger_type,
            SessionVar.ENV:                 self.env_config.env,
            SessionVar.LOAD_TS:             datetime.utcnow().isoformat(),
            SessionVar.CURRENT_USER:        self._get_current_user(),
        }

        for key, value in ctx.items():
            self._spark.conf.set(key, value)

        logger.info(
            "Run context set — business_date=%s  batch_id=%s  log_id=%s",
            business_date, batch_job_config_id, batch_job_log_id
        )

    # ------------------------------------------------------------------
    # Getters — typed accessors used by other modules
    # ------------------------------------------------------------------

    def get_business_date(self) -> date:
        raw = self._spark.conf.get(SessionVar.BUSINESS_DATE)
        return date.fromisoformat(raw)

    def get_batch_job_config_id(self) -> int:
        return int(self._spark.conf.get(SessionVar.BATCH_JOB_CONFIG_ID))

    def get_batch_job_log_id(self) -> str:
        return self._spark.conf.get(SessionVar.BATCH_JOB_LOG_ID)

    def get_trigger_type(self) -> str:
        return self._spark.conf.get(SessionVar.TRIGGER_TYPE)

    def get_load_ts(self) -> str:
        return self._spark.conf.get(SessionVar.LOAD_TS)

    # ------------------------------------------------------------------
    # Databricks widget / job parameter resolution
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_widget_param(
        spark: SparkSession,
        param_name: str,
        default: Optional[str] = None
    ) -> Optional[str]:
        """
        Reads a Databricks widget value or Airflow-injected job parameter.
        Falls back to `default` if the widget/param is not set.

        Airflow passes params to Databricks jobs as notebook parameters,
        which are available via dbutils.widgets.get().
        """
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(spark)
            value = dbutils.widgets.get(param_name)
            return value if value else default
        except Exception:
            return default

    @staticmethod
    def resolve_business_date(spark: SparkSession) -> date:
        """
        Resolves business_date from widget → session var → today (fallback).
        Airflow DAGs should always pass business_date as a widget parameter.
        """
        raw = SessionManager.resolve_widget_param(spark, "business_date")
        if raw:
            return date.fromisoformat(raw)
        # Fallback: try session conf
        try:
            return date.fromisoformat(spark.conf.get(SessionVar.BUSINESS_DATE))
        except Exception:
            return date.today()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_current_user(self) -> str:
        try:
            tags = self._spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.clusterCreatorUserName", "unknown")
            return tags
        except Exception:
            return "unknown"

# =============================================================================
# FILE    : framework/modules/session_manager.py (DATABRICKS FREE TIER)
# MODULE  : 2 — Spark Session & Session-Level Variables
# PURPOSE : Works on Databricks free tier without AWS (DynamoDB/S3).
#           Uses Databricks SQL temp views, widgets, and Spark conf.
#           All storage is within Databricks workspace.
# =============================================================================

from pyspark.sql import SparkSession
from datetime import date, datetime
from typing import Optional, Dict, Any
import logging

from app_config import EnvironmentConfig, FrameworkDefaults

logger = logging.getLogger("etl.session_manager")


# =============================================================================
# SESSION-LEVEL VARIABLE KEYS
# =============================================================================

class SessionVar:
    BUSINESS_DATE       = "etl.business_date"
    BATCH_JOB_CONFIG_ID = "etl.batch_job_config_id"
    BATCH_JOB_LOG_ID    = "etl.batch_job_log_id"
    TRIGGER_TYPE        = "etl.trigger_type"
    ENV                 = "etl.env"
    CURRENT_USER        = "etl.current_user"
    LOAD_TS             = "etl.load_ts"

    # All keys for iteration
    ALL_KEYS = [
        BUSINESS_DATE,
        BATCH_JOB_CONFIG_ID,
        BATCH_JOB_LOG_ID,
        TRIGGER_TYPE,
        ENV,
        CURRENT_USER,
        LOAD_TS,
    ]


# =============================================================================
# STORAGE BACKENDS — pluggable, no external dependencies
# =============================================================================

class StateStore:
    """Abstract interface for storing/retrieving session state."""

    def save(self, batch_job_log_id: str, state: Dict[str, Any]) -> None:
        """Persist state."""
        raise NotImplementedError

    def load(self, batch_job_log_id: str) -> Dict[str, Any]:
        """Retrieve state. Return {} if not found."""
        raise NotImplementedError

    def delete(self, batch_job_log_id: str) -> None:
        """Remove state."""
        raise NotImplementedError


# =============================================================================
# OPTION 1: IN-MEMORY STORE (simplest, no setup needed)
# =============================================================================

class InMemoryStateStore(StateStore):
    """
    Stores variables in a Python dict (in-memory only).
    Best for: Single-job runs, testing, dev environments.
    Limitation: Lost if job restarts or Spark cluster is restarted.
    """

    _store: Dict[str, Dict[str, Any]] = {}

    def save(self, batch_job_log_id: str, state: Dict[str, Any]) -> None:
        """Save to in-memory dict."""
        self._store[batch_job_log_id] = state
        logger.info(f"[InMemory] State saved — log_id: {batch_job_log_id}")

    def load(self, batch_job_log_id: str) -> Dict[str, Any]:
        """Load from in-memory dict."""
        result = self._store.get(batch_job_log_id, {})
        if result:
            logger.info(f"[InMemory] State loaded — log_id: {batch_job_log_id}")
        return result

    def delete(self, batch_job_log_id: str) -> None:
        """Delete from in-memory dict."""
        self._store.pop(batch_job_log_id, None)
        logger.info(f"[InMemory] State deleted — log_id: {batch_job_log_id}")


# =============================================================================
# OPTION 2: DATABRICKS WIDGETS (great for parameter passing)
# =============================================================================

class DatabricksWidgetStore(StateStore):
    """
    Stores variables as Databricks widgets.
    Best for: Multi-notebook workflows, interactive dashboards.
    Limitation: Widgets are text-only; must serialize/deserialize.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._ensure_widgets_exist()

    def _ensure_widgets_exist(self) -> None:
        """Create widgets if they don't exist."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(self.spark)

            for var_name in SessionVar.ALL_KEYS:
                try:
                    # Create widget if it doesn't exist
                    dbutils.widgets.text(var_name, "")
                except Exception:
                    # Widget already exists
                    pass

            logger.info("[Widget] All widgets initialized")
        except Exception as e:
            logger.warning(f"[Widget] Could not initialize widgets: {e}")

    def save(self, batch_job_log_id: str, state: Dict[str, Any]) -> None:
        """Save all state vars to widgets."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(self.spark)

            for key, value in state.items():
                # Store as JSON-serialized string
                dbutils.widgets.removeOption(key)
                dbutils.widgets.text(key, str(value))

            logger.info(f"[Widget] State saved to widgets — log_id: {batch_job_log_id}")
        except Exception as e:
            logger.error(f"[Widget] Failed to save to widgets: {e}")

    def load(self, batch_job_log_id: str) -> Dict[str, Any]:
        """Load state from widgets."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(self.spark)

            state = {}
            for var_name in SessionVar.ALL_KEYS:
                try:
                    value = dbutils.widgets.get(var_name)
                    if value:
                        state[var_name] = value
                except Exception:
                    pass

            if state:
                logger.info(f"[Widget] State loaded from widgets")
            return state
        except Exception as e:
            logger.warning(f"[Widget] Failed to load from widgets: {e}")
            return {}

    def delete(self, batch_job_log_id: str) -> None:
        """Clear all widget values."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(self.spark)

            for var_name in SessionVar.ALL_KEYS:
                try:
                    dbutils.widgets.removeOption(var_name)
                except Exception:
                    pass

            logger.info("[Widget] All widgets cleared")
        except Exception as e:
            logger.error(f"[Widget] Failed to clear widgets: {e}")


# =============================================================================
# OPTION 3: DATABRICKS WORKSPACE VOLUMES (persistent, local to workspace)
# =============================================================================

class DatabricksVolumesStore(StateStore):
    """
    Stores session state as JSON files in Databricks Volumes.
    Best for: Multi-job persistence within a workspace.
    Limitation: Only available in Databricks (not open-source Spark).
    Note: Requires /Volumes path (Databricks 12.0+).
    """

    def __init__(self, spark: SparkSession, volume_path: str = "/Volumes/Shared/default/etl_state"):
        self.spark = spark
        self.volume_path = volume_path
        self._ensure_volume_exists()

    def _ensure_volume_exists(self) -> None:
        """Create volume directory if it doesn't exist."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            import json
            dbutils = DBUtils(self.spark)

            # Test if we can write to the path
            test_file = f"{self.volume_path}/.init"
            dbutils.fs.put(test_file, "{}", overwrite=True)
            dbutils.fs.rm(test_file)
            logger.info(f"[Volumes] Initialized — path: {self.volume_path}")
        except Exception as e:
            logger.warning(f"[Volumes] Could not initialize: {e}. Ensure Volumes are enabled.")

    def save(self, batch_job_log_id: str, state: Dict[str, Any]) -> None:
        """Save state as JSON to Volumes."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            import json
            dbutils = DBUtils(self.spark)

            file_path = f"{self.volume_path}/{batch_job_log_id}/state.json"
            payload = json.dumps(state, indent=2, default=str)
            dbutils.fs.put(file_path, payload, overwrite=True)
            logger.info(f"[Volumes] State saved — path: {file_path}")
        except Exception as e:
            logger.error(f"[Volumes] Failed to save state: {e}")

    def load(self, batch_job_log_id: str) -> Dict[str, Any]:
        """Load state from Volumes."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            import json
            dbutils = DBUtils(self.spark)

            file_path = f"{self.volume_path}/{batch_job_log_id}/state.json"
            content = dbutils.fs.head(file_path)
            state = json.loads(content)
            logger.info(f"[Volumes] State loaded — path: {file_path}")
            return state
        except Exception as e:
            logger.debug(f"[Volumes] State not found or error: {e}")
            return {}

    def delete(self, batch_job_log_id: str) -> None:
        """Delete state from Volumes."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(self.spark)

            file_path = f"{self.volume_path}/{batch_job_log_id}"
            dbutils.fs.rm(file_path, recurse=True)
            logger.info(f"[Volumes] State deleted — path: {file_path}")
        except Exception as e:
            logger.warning(f"[Volumes] Could not delete state: {e}")


# =============================================================================
# OPTION 4: DELTA TABLE (scalable, queryable)
# =============================================================================

class DeltaTableStateStore(StateStore):
    """
    Stores session state in a Delta table.
    Best for: Large-scale deployments, audit trails, data warehouse integration.
    Limitation: Requires write access to workspace default catalog.
    """

    def __init__(self, spark: SparkSession, table_name: str = "etl_session_state"):
        self.spark = spark
        self.table_name = table_name
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create Delta table if it doesn't exist."""
        try:
            # Create schema if doesn't exist
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS default")

            # Create table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    batch_job_log_id STRING PRIMARY KEY,
                    business_date STRING,
                    batch_job_config_id STRING,
                    trigger_type STRING,
                    env STRING,
                    current_user STRING,
                    load_ts STRING,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    CONSTRAINT unique_log_id UNIQUE(batch_job_log_id)
                )
                USING DELTA
            """)
            logger.info(f"[Delta] Table initialized — {self.table_name}")
        except Exception as e:
            logger.warning(f"[Delta] Table creation failed (may already exist): {e}")

    def save(self, batch_job_log_id: str, state: Dict[str, Any]) -> None:
        """Save state to Delta table."""
        try:
            # Prepare row
            row_dict = {
                "batch_job_log_id": batch_job_log_id,
                "business_date": state.get(SessionVar.BUSINESS_DATE, ""),
                "batch_job_config_id": state.get(SessionVar.BATCH_JOB_CONFIG_ID, ""),
                "trigger_type": state.get(SessionVar.TRIGGER_TYPE, ""),
                "env": state.get(SessionVar.ENV, ""),
                "current_user": state.get(SessionVar.CURRENT_USER, ""),
                "load_ts": state.get(SessionVar.LOAD_TS, ""),
            }

            # Upsert into table
            df = self.spark.createDataFrame([row_dict])
            df.write.format("delta").mode("overwrite").option(
                "mergeSchema", "true"
            ).saveAsTable(self.table_name)

            logger.info(f"[Delta] State saved — log_id: {batch_job_log_id}")
        except Exception as e:
            logger.error(f"[Delta] Failed to save state: {e}")

    def load(self, batch_job_log_id: str) -> Dict[str, Any]:
        """Load state from Delta table."""
        try:
            result = self.spark.sql(
                f"SELECT * FROM {self.table_name} WHERE batch_job_log_id = '{batch_job_log_id}'"
            ).collect()

            if result:
                row = result[0]
                state = {
                    SessionVar.BUSINESS_DATE: row.business_date or "",
                    SessionVar.BATCH_JOB_CONFIG_ID: row.batch_job_config_id or "",
                    SessionVar.BATCH_JOB_LOG_ID: batch_job_log_id,
                    SessionVar.TRIGGER_TYPE: row.trigger_type or "",
                    SessionVar.ENV: row.env or "",
                    SessionVar.CURRENT_USER: row.current_user or "",
                    SessionVar.LOAD_TS: row.load_ts or "",
                }
                logger.info(f"[Delta] State loaded — log_id: {batch_job_log_id}")
                return state
            return {}
        except Exception as e:
            logger.debug(f"[Delta] Failed to load state: {e}")
            return {}

    def delete(self, batch_job_log_id: str) -> None:
        """Delete state from Delta table."""
        try:
            self.spark.sql(
                f"DELETE FROM {self.table_name} WHERE batch_job_log_id = '{batch_job_log_id}'"
            )
            logger.info(f"[Delta] State deleted — log_id: {batch_job_log_id}")
        except Exception as e:
            logger.error(f"[Delta] Failed to delete state: {e}")


# =============================================================================
# SPARK SESSION MANAGER (DATABRICKS FREE TIER)
# =============================================================================

class SessionManager:
    """
    Manages session-level variables for Databricks free tier.
    No external AWS dependencies. Works entirely within Databricks.

    Usage:
        # Option 1: In-memory (simplest)
        sm = SessionManager(env_config, InMemoryStateStore())
        
        # Option 2: Widgets (for multi-notebook workflows)
        sm = SessionManager(env_config, DatabricksWidgetStore(spark))
        
        # Option 3: Volumes (persistent, Databricks 12.0+)
        sm = SessionManager(env_config, DatabricksVolumesStore(spark))
        
        # Option 4: Delta table (scalable)
        sm = SessionManager(env_config, DeltaTableStateStore(spark))

        spark = sm.get_session()
        sm.set_run_context(
            spark=spark,
            business_date=date(2025, 7, 24),
            batch_job_config_id=1,
            batch_job_log_id="1_20250724090000",
            trigger_type="SCHEDULED"
        )
    """

    def __init__(
        self,
        env_config: Optional[EnvironmentConfig] = None,
        state_store: Optional[StateStore] = None,
    ):
        self.env_config = env_config or EnvironmentConfig()
        self.state_store = state_store or InMemoryStateStore()
        self._spark: Optional[SparkSession] = None
        self._local_state: Dict[str, Any] = {}
        self._batch_job_log_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Session retrieval
    # ------------------------------------------------------------------

    def get_session(self) -> SparkSession:
        """Returns the active SparkSession."""
        if self._spark is None:
            self._spark = SparkSession.getActiveSession()
            if self._spark is None:
                self._spark = SparkSession.builder.getOrCreate()
        
        logger.info(f"Using SparkSession — app: ")
        return self._spark

    # ------------------------------------------------------------------
    # Run-context setter
    # ------------------------------------------------------------------

    def set_run_context(
        self,
        spark: SparkSession,
        business_date: date,
        batch_job_config_id: int,
        batch_job_log_id: str,
        trigger_type: str = "SCHEDULED",
    ) -> None:
        """
        Persists run-level variables to state store AND creates Spark SQL temp views.
        """
        self._spark = spark
        self._batch_job_log_id = batch_job_log_id

        # Build state dict
        state = {
            SessionVar.BUSINESS_DATE: str(business_date),
            SessionVar.BATCH_JOB_CONFIG_ID: str(batch_job_config_id),
            SessionVar.BATCH_JOB_LOG_ID: batch_job_log_id,
            SessionVar.TRIGGER_TYPE: trigger_type,
            SessionVar.ENV: self.env_config.env,
            SessionVar.LOAD_TS: datetime.utcnow().isoformat(),
            SessionVar.CURRENT_USER: self._get_current_user(spark),
        }

        # Cache locally for fast access
        self._local_state = state

        # Persist to state store
        self.state_store.save(batch_job_log_id, state)

        # Create Spark SQL temp views for SQL access
        self._create_temp_views(spark, state)

        logger.info(
            f"Run context set — business_date={business_date}  "
            f"batch_id={batch_job_config_id}  log_id={batch_job_log_id}"
        )

    def _create_temp_views(self, spark: SparkSession, state: Dict[str, Any]) -> None:
        """Create Spark SQL temporary views for variable access."""
        try:
            rows = [(key, value) for key, value in state.items()]
            df = spark.createDataFrame(rows, ["key", "value"])
            df.createOrReplaceTempView("spark_session_vars")
            logger.info("Created temp view: spark_session_vars")
        except Exception as e:
            logger.error(f"Failed to create temp view: {e}")

    # ------------------------------------------------------------------
    # Restore run context
    # ------------------------------------------------------------------

    def load_run_context(self, spark: SparkSession, batch_job_log_id: str) -> None:
        """Restores a previously saved run context."""
        self._spark = spark
        self._batch_job_log_id = batch_job_log_id

        state = self.state_store.load(batch_job_log_id)
        if not state:
            logger.warning(
                f"No saved state found for batch_job_log_id={batch_job_log_id}. "
                "Call set_run_context() instead."
            )
            return

        self._local_state = state
        self._create_temp_views(spark, state)

        logger.info(f"Run context restored — log_id: {batch_job_log_id}")

    # ------------------------------------------------------------------
    # Typed getters
    # ------------------------------------------------------------------

    def get_business_date(self) -> date:
        raw = self._local_state.get(SessionVar.BUSINESS_DATE)
        if not raw:
            raise RuntimeError("Business date not set. Call set_run_context() first.")
        return date.fromisoformat(raw)

    def get_batch_job_config_id(self) -> int:
        raw = self._local_state.get(SessionVar.BATCH_JOB_CONFIG_ID)
        if not raw:
            raise RuntimeError("Batch job config ID not set. Call set_run_context() first.")
        return int(raw)

    def get_batch_job_log_id(self) -> str:
        raw = self._local_state.get(SessionVar.BATCH_JOB_LOG_ID)
        if not raw:
            raise RuntimeError("Batch job log ID not set. Call set_run_context() first.")
        return raw

    def get_trigger_type(self) -> str:
        return self._local_state.get(SessionVar.TRIGGER_TYPE, "SCHEDULED")

    def get_load_ts(self) -> str:
        return self._local_state.get(SessionVar.LOAD_TS, "")

    def get_current_user(self) -> str:
        return self._local_state.get(SessionVar.CURRENT_USER, "unknown")

    def get_env(self) -> str:
        return self._local_state.get(SessionVar.ENV, "unknown")

    # ------------------------------------------------------------------
    # SQL access
    # ------------------------------------------------------------------

    def get_from_sql(self, spark: SparkSession, key: str) -> Optional[str]:
        """Retrieve a session variable using Spark SQL."""
        try:
            result = spark.sql(
                f"SELECT value FROM spark_session_vars WHERE key = '{key}'"
            ).collect()
            if result:
                return result[0]["value"]
            return None
        except Exception as e:
            logger.warning(f"Failed to query session var via SQL: {e}")
            return None

    # ------------------------------------------------------------------
    # Widget resolution (Databricks-specific)
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_widget_param(
        spark: SparkSession,
        param_name: str,
        default: Optional[str] = None,
    ) -> Optional[str]:
        """Reads a Databricks widget value."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(spark)
            value = dbutils.widgets.get(param_name)
            return value if value else default
        except Exception:
            return default

    @staticmethod
    def resolve_business_date(spark: SparkSession) -> date:
        """Resolves business_date from widget → today."""
        raw = SessionManager.resolve_widget_param(spark, "business_date")
        if raw:
            try:
                return date.fromisoformat(raw)
            except ValueError:
                pass
        return date.today()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self) -> None:
        """Delete state after job completes."""
        if self._batch_job_log_id:
            self.state_store.delete(self._batch_job_log_id)
            logger.info(f"Session state cleaned up — log_id: {self._batch_job_log_id}")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_current_user(self, spark: SparkSession) -> str:
        try:
            tags = spark.sparkContext.getConf().get(
                "spark.databricks.clusterUsageTags.clusterCreatorUserName", "unknown"
            )
            return tags if tags else "unknown"
        except Exception:
            return "unknown"
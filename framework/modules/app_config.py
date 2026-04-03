# =============================================================================
# FILE    : framework/modules/app_config.py
# MODULE  : 7 — Application-Level Variables & Constants
# PURPOSE : Central place for all framework-wide constants, environment names,
#           layer names, status codes, and configurable defaults.
#           Import this module first in every other module.
# =============================================================================

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional
import os


# =============================================================================
# ENUMERATIONS — use these everywhere instead of raw strings
# =============================================================================

class Layer(str, Enum):
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD   = "GOLD"

class LoadStrategy(str, Enum):
    FULL   = "FULL"
    INC    = "INC"          # Incremental (watermark-based)
    SCD1   = "SCD1"         # Slowly Changing Dimension Type 1
    SCD2   = "SCD2"         # Slowly Changing Dimension Type 2
    APPEND = "APPEND"       # Insert-only / append

class JobStatus(str, Enum):
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"

class TriggerType(str, Enum):
    SCHEDULED = "SCHEDULED"
    MANUAL    = "MANUAL"
    RERUN     = "RERUN"
    API       = "API"

class ConnectionType(str, Enum):
    JDBC     = "JDBC"
    SFTP     = "SFTP"
    ADLS     = "ADLS"
    REST_API = "REST_API"
    KAFKA    = "KAFKA"
    DELTA    = "DELTA"

class Frequency(str, Enum):
    DAILY   = "DAILY"
    WEEKLY  = "WEEKLY"
    MONTHLY = "MONTHLY"
    ADHOC   = "ADHOC"


# =============================================================================
# ENVIRONMENT CONFIG
# =============================================================================

class Environment(str, Enum):
    DEV  = "dev"
    UAT  = "uat"
    PROD = "prod"


@dataclass
class EnvironmentConfig:
    """
    Environment-specific settings resolved at runtime.
    Set ETL_ENV environment variable or pass via Databricks widget.
    """
    env:                    str     = field(default_factory=lambda: os.getenv("ETL_ENV", "dev"))
    metadata_schema:        str     = "etl_metadata"
    key_vault_scope:        str     = field(init=False)
    databricks_secret_scope:str     = field(init=False)
    default_catalog:        str     = field(init=False)

    def __post_init__(self):
        env = self.env.lower()
        self.key_vault_scope         = f"etl-kv-scope-{env}"
        self.databricks_secret_scope = f"etl-secret-scope-{env}"
        self.default_catalog         = f"etl_catalog_{env}"


# =============================================================================
# METADATA TABLE NAMES — single source of truth
# =============================================================================

class MetadataTable:
    BATCH_JOB_CONFIG    = "etl_metadata.batch_job_config"
    JOB_CONFIG          = "etl_metadata.job_config"
    CONNECTION_CONFIG   = "etl_metadata.etl_connection_config"
    BATCH_JOB_LOG       = "etl_metadata.batch_job_log"
    JOB_AUDIT_LOG       = "etl_metadata.job_audit_log"


# =============================================================================
# FRAMEWORK DEFAULTS
# =============================================================================

class FrameworkDefaults:
    # Retry behaviour
    MAX_RETRY_ATTEMPTS      = 2
    RETRY_DELAY_SECONDS     = 300

    # Parallelism
    MAX_PARALLEL_JOBS       = 5

    # Watermark
    DEFAULT_WATERMARK_DATE  = "1900-01-01"

    # Notebook SQL directory (relative to repo root)
    SQL_NOTEBOOK_BASE_PATH  = "framework/sql"

    # Notebook naming convention:
    #   {layer}/{source_system}/{layer_prefix}_{source_system}_{table_name}
    # e.g. bronze/visionplus/brz_visionplus_ath2_lrpmt
    NOTEBOOK_PREFIX = {
        Layer.BRONZE: "brz",
        Layer.SILVER: "slv",
        Layer.GOLD:   "gld",
    }

    # Spark write modes
    WRITE_MODE_OVERWRITE = "overwrite"
    WRITE_MODE_APPEND    = "append"

    # Delta merge alias
    MERGE_SOURCE_ALIAS = "src"
    MERGE_TARGET_ALIAS = "tgt"

    # Audit columns added to every Delta table
    AUDIT_COLUMNS = [
        "etl_batch_job_log_id",
        "etl_job_config_id",
        "etl_business_date",
        "etl_load_ts",
        "etl_load_strategy",
    ]

    # Schedule timezone default
    DEFAULT_TIMEZONE = "UTC"


# =============================================================================
# NOTEBOOK NAMING CONVENTION HELPER
# =============================================================================

def resolve_notebook_path(layer: Layer, source_system: str, table_name: str) -> str:
    """
    Derive the canonical notebook path from metadata.

    Convention:
        framework/sql/{layer}/{source_system_lower}/{prefix}_{source_system_lower}_{table_lower}

    Examples:
        BRONZE, visionplus, ATH2_LRPMT_TGT
        → framework/sql/bronze/visionplus/brz_visionplus_ath2_lrpmt_tgt

        SILVER, shared, CONSOLIDATED_ACCOUNTS
        → framework/sql/silver/shared/slv_shared_consolidated_accounts
    """
    prefix         = FrameworkDefaults.NOTEBOOK_PREFIX[layer]
    layer_str      = layer.value.lower()
    system_str     = source_system.lower().replace(" ", "_")
    table_str      = table_name.lower()
    notebook_name  = f"{prefix}_{system_str}_{table_str}"
    return f"{FrameworkDefaults.SQL_NOTEBOOK_BASE_PATH}/{layer_str}/{system_str}/{notebook_name}"

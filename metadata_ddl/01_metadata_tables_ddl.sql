-- =============================================================================
-- FILE    : 01_metadata_tables_ddl.sql
-- PURPOSE : DDL for all ETL framework metadata tables
--           3 config tables (one-time setup) + 2 runtime logging tables
-- SCHEMA  : etl_metadata
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS etl_metadata
COMMENT 'ETL Framework control and logging schema';

-- =============================================================================
-- CONFIG TABLE 1: batch_job_config
-- PURPOSE : Defines a logical batch (group of tables) and its Airflow schedule.
--           One batch = one Airflow DAG.
-- POPULATED: One-time setup / when a new source system onboarded.
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_metadata.batch_job_config (
    batch_job_config_id     BIGINT          NOT NULL    COMMENT 'Surrogate PK, auto-incremented',
    source_system           STRING          NOT NULL    COMMENT 'Source system name (e.g. VisionPlus, SAP, Salesforce)',
    batch_name              STRING          NOT NULL    COMMENT 'Short batch identifier used in DAG naming (e.g. VP-1)',
    batch_desc              STRING                      COMMENT 'Business description of what this batch loads',
    layer                   STRING          NOT NULL    COMMENT 'Target layer transition: BRONZE | SILVER | GOLD | EL_TO_AEL',
    frequency               STRING          NOT NULL    COMMENT 'Schedule frequency: DAILY | WEEKLY | MONTHLY | ADHOC',
    airflow_dag_name        STRING          NOT NULL    COMMENT 'Airflow DAG ID that drives this batch',
    schedule_cron           STRING                      COMMENT 'Cron expression (e.g. 0 2 * * *); NULL = triggered externally',
    schedule_timezone       STRING          DEFAULT 'UTC' COMMENT 'Timezone for schedule (e.g. Asia/Kolkata)',
    max_parallel_jobs       INT             DEFAULT 5   COMMENT 'Max tables to execute in parallel within this batch',
    retry_attempts          INT             DEFAULT 2   COMMENT 'Auto-retry count on failure',
    retry_delay_seconds     INT             DEFAULT 300 COMMENT 'Seconds to wait between retries',
    alert_email             STRING                      COMMENT 'Comma-separated alert email addresses',
    is_active               BOOLEAN         DEFAULT TRUE COMMENT 'Soft-disable flag; FALSE = batch skipped by orchestrator',
    created_by              STRING                      COMMENT 'User who created this config record',
    created_dt              TIMESTAMP       DEFAULT current_timestamp(),
    updated_by              STRING,
    updated_dt              TIMESTAMP       DEFAULT current_timestamp(),
    CONSTRAINT pk_batch_job_config PRIMARY KEY (batch_job_config_id)
)
USING DELTA
COMMENT 'CONFIG TABLE 1 of 3 — Batch-level config: one row per logical batch / Airflow DAG'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');


-- =============================================================================
-- CONFIG TABLE 2: job_config
-- PURPOSE : Table-level ETL configuration within a batch.
--           Defines source→target mapping, load strategy, notebook, ordering.
-- POPULATED: One-time setup / when a new table is onboarded.
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_metadata.job_config (
    job_config_id           BIGINT          NOT NULL    COMMENT 'Surrogate PK, auto-incremented',
    batch_job_config_id     BIGINT          NOT NULL    COMMENT 'FK → batch_job_config.batch_job_config_id',

    -- Source definition
    source_system           STRING                      COMMENT 'Overrides batch-level source_system if different',
    source_schema           STRING                      COMMENT 'Source schema/database name',
    source_table_name       STRING                      COMMENT 'Source table name (NULL if fully custom SQL)',
    source_watermark_column STRING                      COMMENT 'Column used for incremental high-watermark (e.g. updated_at)',
    source_watermark_value  STRING                      COMMENT 'Last loaded watermark value (updated after each run)',
    connection_id           STRING                      COMMENT 'FK → etl_connection_config.connection_id',

    -- Target definition
    target_schema           STRING          NOT NULL    COMMENT 'Target Delta schema (e.g. cdm_visionplus)',
    target_table_name       STRING          NOT NULL    COMMENT 'Target Delta table name (e.g. ATH2_LRPMT_TGT)',
    primary_key_columns     STRING                      COMMENT 'Comma-separated PK columns for MERGE/SCD logic',
    partition_columns       STRING                      COMMENT 'Comma-separated columns for Delta PARTITIONED BY',

    -- Transformation
    notebook_name           STRING          NOT NULL    COMMENT 'Transformation notebook path relative to sql/ directory',
    target_frequency        STRING          NOT NULL    COMMENT 'DAILY | WEEKLY | MONTHLY | ADHOC',
    table_strategy          STRING          NOT NULL    COMMENT 'FULL | INC (incremental) | SCD1 | SCD2 | APPEND',

    -- Dependency & ordering
    depends_on_job_ids      STRING                      COMMENT 'Comma-separated job_config_ids this job depends on',
    execution_order         INT             DEFAULT 1   COMMENT 'Execution order within batch; same value = run in parallel',

    -- Quality & hooks
    expected_min_row_count  BIGINT                      COMMENT 'DQ check: minimum expected row count post-load',
    pre_hook_notebook       STRING                      COMMENT 'Optional notebook to run before this table load',
    post_hook_notebook      STRING                      COMMENT 'Optional notebook to run after this table load',

    is_active               BOOLEAN         DEFAULT TRUE COMMENT 'FALSE = skip this table during batch execution',
    created_by              STRING,
    created_dt              TIMESTAMP       DEFAULT current_timestamp(),
    updated_by              STRING,
    updated_dt              TIMESTAMP       DEFAULT current_timestamp(),
    CONSTRAINT pk_job_config PRIMARY KEY (job_config_id)
)
USING DELTA
COMMENT 'CONFIG TABLE 2 of 3 — Table-level ETL config: source/target mapping, strategy, notebook, ordering'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');


-- =============================================================================
-- CONFIG TABLE 3: etl_connection_config
-- PURPOSE : Source system connection registry.
--           Actual secrets stored in Azure Key Vault; only KV secret names here.
-- POPULATED: One-time setup per source system.
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_metadata.etl_connection_config (
    connection_id               STRING      NOT NULL    COMMENT 'Unique ID (e.g. CONN_VP_PROD)',
    connection_name             STRING      NOT NULL    COMMENT 'Display name',
    connection_type             STRING      NOT NULL    COMMENT 'JDBC | SFTP | ADLS | REST_API | KAFKA | DELTA',
    host_kv_secret              STRING                  COMMENT 'Key Vault secret name for host/URL',
    port_kv_secret              STRING                  COMMENT 'Key Vault secret name for port',
    username_kv_secret          STRING                  COMMENT 'Key Vault secret name for username',
    password_kv_secret          STRING                  COMMENT 'Key Vault secret name for password',
    database_kv_secret          STRING                  COMMENT 'Key Vault secret name for database name',
    jdbc_driver_class           STRING                  COMMENT 'JDBC driver class (e.g. com.microsoft.sqlserver.jdbc.SQLServerDriver)',
    extra_options_json          STRING                  COMMENT 'JSON of additional JDBC/connection properties',
    is_active                   BOOLEAN     DEFAULT TRUE,
    created_by                  STRING,
    created_dt                  TIMESTAMP   DEFAULT current_timestamp(),
    updated_by                  STRING,
    updated_dt                  TIMESTAMP   DEFAULT current_timestamp(),
    CONSTRAINT pk_etl_connection_config PRIMARY KEY (connection_id)
)
USING DELTA
COMMENT 'CONFIG TABLE 3 of 3 — Source connection registry (secrets via Azure Key Vault)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');


-- =============================================================================
-- LOG TABLE 1: batch_job_log
-- PURPOSE : One row per batch execution (per business_date + DAG run).
--           Written by orchestrator at batch start; updated at batch end.
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_metadata.batch_job_log (
    batch_job_log_id        STRING          NOT NULL    COMMENT 'Composite PK: {batch_job_config_id}_{YYYYMMDDHHMMSS}',
    batch_job_config_id     BIGINT          NOT NULL    COMMENT 'FK → batch_job_config.batch_job_config_id',
    business_date           DATE            NOT NULL    COMMENT 'Logical processing/business date',
    batch_start_date        TIMESTAMP                   COMMENT 'Actual wall-clock batch start time',
    batch_end_date          TIMESTAMP                   COMMENT 'Actual wall-clock batch end time',
    batch_status            STRING          NOT NULL    COMMENT 'RUNNING | COMPLETED | FAILED | SKIPPED',
    batch_airflow_dag_id    STRING                      COMMENT 'Airflow DAG run ID for traceability',
    skip_reason             STRING                      COMMENT 'Populated when batch_status = SKIPPED',
    total_jobs              INT             DEFAULT 0   COMMENT 'Total tables in this batch run',
    jobs_completed          INT             DEFAULT 0,
    jobs_failed             INT             DEFAULT 0,
    jobs_skipped            INT             DEFAULT 0,
    triggered_by            STRING                      COMMENT 'SCHEDULED | MANUAL | RERUN | API',
    databricks_run_id       STRING                      COMMENT 'Databricks workflow run ID',
    error_message           STRING                      COMMENT 'Top-level error message if batch failed',
    created_dt              TIMESTAMP       DEFAULT current_timestamp(),
    updated_dt              TIMESTAMP       DEFAULT current_timestamp(),
    CONSTRAINT pk_batch_job_log PRIMARY KEY (batch_job_log_id)
)
USING DELTA
PARTITIONED BY (business_date)
COMMENT 'LOG TABLE 1 of 2 — Batch-level runtime audit log'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.logRetentionDuration'       = 'interval 180 days'
);


-- =============================================================================
-- LOG TABLE 2: job_audit_log
-- PURPOSE : One row per individual table load within a batch run.
--           Written at table load start; updated at completion/failure.
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_metadata.job_audit_log (
    job_audit_log_id        BIGINT          NOT NULL    COMMENT 'Surrogate PK: generated as YYYYMMDDHHMMSSmmm + seq',
    business_date           DATE            NOT NULL    COMMENT 'Logical processing/business date',
    job_config_id           BIGINT          NOT NULL    COMMENT 'FK → job_config.job_config_id',
    batch_job_config_id     BIGINT          NOT NULL    COMMENT 'FK → batch_job_config.batch_job_config_id',
    batch_airflow_dag_id    STRING                      COMMENT 'FK → batch_job_log.batch_airflow_dag_id',
    batch_job_log_id        STRING                      COMMENT 'FK → batch_job_log.batch_job_log_id',
    job_status              STRING          NOT NULL    COMMENT 'RUNNING | COMPLETED | FAILED | SKIPPED',
    skip_reason             STRING                      COMMENT 'Reason when job_status = SKIPPED',
    job_start_date          TIMESTAMP                   COMMENT 'Table load start time',
    job_end_date            TIMESTAMP                   COMMENT 'Table load end time',
    duration_seconds        INT                         COMMENT 'Computed: job_end_date - job_start_date in seconds',
    source_row_count        BIGINT                      COMMENT 'Rows read from source',
    target_rows_inserted    BIGINT                      COMMENT 'Net new rows inserted into target',
    target_rows_updated     BIGINT                      COMMENT 'Rows updated/merged in target',
    target_rows_deleted     BIGINT                      COMMENT 'Rows deleted (SCD2 / hard deletes)',
    target_total_row_count  BIGINT                      COMMENT 'Total rows in target table after load',
    watermark_value_used    STRING                      COMMENT 'Watermark value at start of this run',
    watermark_value_new     STRING                      COMMENT 'New high watermark after successful load',
    error_desc              STRING                      COMMENT 'Error description on failure; "Successful Run" on success',
    notebook_path           STRING                      COMMENT 'Actual notebook path executed',
    retry_attempt           INT             DEFAULT 0   COMMENT '0 = first attempt; 1,2 = retries',
    databricks_task_run_id  STRING                      COMMENT 'Databricks task-level run ID',
    created_dt              TIMESTAMP       DEFAULT current_timestamp(),
    updated_dt              TIMESTAMP       DEFAULT current_timestamp(),
    CONSTRAINT pk_job_audit_log PRIMARY KEY (job_audit_log_id)
)
USING DELTA
PARTITIONED BY (business_date)
COMMENT 'LOG TABLE 2 of 2 — Table-level runtime audit log with row counts and watermarks'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.logRetentionDuration'       = 'interval 180 days'
);


-- =============================================================================
-- SAMPLE CONFIG INSERT (seed data to match xlsx examples)
-- =============================================================================

INSERT INTO etl_metadata.batch_job_config
    (batch_job_config_id, source_system, batch_name, batch_desc, layer, frequency, airflow_dag_name, is_active, created_by)
VALUES
    (1, 'VisionPlus', 'VP-1', 'First batch of VP',  'EL_TO_AEL', 'Daily', 'dag_visionplus_vp1', TRUE, 'framework_init'),
    (2, 'VisionPlus', 'VP-2', 'Second batch of VP', 'EL_TO_AEL', 'Daily', 'dag_visionplus_vp2', TRUE, 'framework_init');

INSERT INTO etl_metadata.job_config
    (job_config_id, batch_job_config_id, target_schema, target_table_name, notebook_name, target_frequency, table_strategy, execution_order, is_active, created_by)
VALUES
    (1, 1, 'cdm_visionplus', 'ATH2_LRPMT_TGT',  'bronze/visionplus/brz_visionplus_ath2_lrpmt',  'DAILY', 'FULL', 1, TRUE, 'framework_init'),
    (2, 1, 'cdm_visionplus', 'ATPTRPT_TGT',      'bronze/visionplus/brz_visionplus_atptrpt',     'DAILY', 'INC',  1, TRUE, 'framework_init'),
    (3, 1, 'cdm_visionplus', 'ATH3X_TGT',        'bronze/visionplus/brz_visionplus_ath3x',       'DAILY', 'INC',  1, TRUE, 'framework_init'),
    (4, 1, 'cdm_visionplus', 'ATH3D_TGT',        'bronze/visionplus/brz_visionplus_ath3d',       'DAILY', 'FULL', 2, TRUE, 'framework_init');

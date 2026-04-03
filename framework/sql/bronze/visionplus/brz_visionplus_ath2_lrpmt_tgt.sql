-- =============================================================================
-- FILE    : framework/sql/bronze/visionplus/brz_visionplus_ath2_lrpmt_tgt.sql
-- LAYER   : BRONZE
-- SOURCE  : VisionPlus
-- TABLE   : ATH2_LRPMT_TGT
-- STRATEGY: FULL
-- PURPOSE : Raw extraction of ATH2_LRPMT from VisionPlus source.
--           Bronze = as-is from source, no business transformation.
--           Only adds ETL audit columns (handled by transformation engine).
--
-- NAMING CONVENTION:
--   {layer_prefix}_{source_system}_{target_table_name}.sql
--   brz = bronze | slv = silver | gld = gold
--
-- MULTI-QUERY CONVENTION:
--   Each logical step is prefixed with: -- query_NNN_step_name
--   The LAST query must produce the final result DataFrame / temp view.
--   The notebook must exit with the temp view name:
--       dbutils.notebook.exit("query_003_final")
-- =============================================================================

-- query_001_extract
-- Reads raw data from source via the registered JDBC temp view.
-- The source DataFrame is registered as 'src_ath2_lrpmt' by the
-- connection_manager before this notebook is invoked.
SELECT
    -- Primary identifiers
    ACCT_ID,
    SEQ_NO,
    PMT_DT,

    -- Payment detail columns
    PMT_AMT,
    PMT_TYPE_CD,
    PMT_STATUS_CD,
    ORIG_PMT_AMT,
    CURR_CD,
    POST_DT,
    EFFECTIVE_DT,

    -- Housekeeping columns from source
    CREATE_DT,
    UPDATE_DT,
    LOAD_DT

FROM src_ath2_lrpmt
;


-- query_002_cleanse
-- Light bronze-layer cleansing: trim string columns, handle source-level NULLs.
-- NO business logic here — that lives in silver.
SELECT
    TRIM(ACCT_ID)           AS ACCT_ID,
    TRIM(SEQ_NO)            AS SEQ_NO,
    PMT_DT,
    PMT_AMT,
    TRIM(PMT_TYPE_CD)       AS PMT_TYPE_CD,
    TRIM(PMT_STATUS_CD)     AS PMT_STATUS_CD,
    ORIG_PMT_AMT,
    TRIM(CURR_CD)           AS CURR_CD,
    POST_DT,
    EFFECTIVE_DT,
    CREATE_DT,
    UPDATE_DT,
    LOAD_DT,

    -- Source system tag for Silver consolidation
    'VisionPlus'            AS SRC_SYSTEM_NM,

    -- Parameters injected by transformation engine
    CAST('${business_date}' AS DATE)  AS SRC_BUSINESS_DT

FROM query_001_extract
;


-- query_003_final
-- Final select — this is the DataFrame written to the Delta Bronze table.
-- Column names match the target Delta schema for ATH2_LRPMT_TGT.
SELECT
    ACCT_ID,
    SEQ_NO,
    PMT_DT,
    PMT_AMT,
    PMT_TYPE_CD,
    PMT_STATUS_CD,
    ORIG_PMT_AMT,
    CURR_CD,
    POST_DT,
    EFFECTIVE_DT,
    CREATE_DT,
    UPDATE_DT,
    LOAD_DT,
    SRC_SYSTEM_NM,
    SRC_BUSINESS_DT
FROM query_002_cleanse
;

-- dbutils.notebook.exit("query_003_final")

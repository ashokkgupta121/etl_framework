-- =============================================================================
-- FILE    : framework/sql/bronze/visionplus/brz_visionplus_atptrpt_tgt.sql
-- LAYER   : BRONZE
-- SOURCE  : VisionPlus
-- TABLE   : ATPTRPT_TGT
-- STRATEGY: INC (Incremental — watermark on UPDATE_DT)
-- PURPOSE : Incremental extraction of ATPTRPT from VisionPlus.
--           Only rows with UPDATE_DT > last watermark are pulled.
-- =============================================================================

-- query_001_extract_incremental
-- Incremental pull using watermark parameter injected by the engine.
-- ${watermark_value} is replaced at runtime with the last loaded value.
SELECT
    ACCT_ID,
    RPT_DT,
    RPT_TYPE_CD,
    RPT_STATUS_CD,
    RPT_AMT,
    CURR_CD,
    POSTING_DT,
    REVERSAL_IND,
    ORIG_RPT_ID,
    CREATE_DT,
    UPDATE_DT

FROM src_atptrpt
WHERE UPDATE_DT > CAST('${watermark_value}' AS TIMESTAMP)
;


-- query_002_cleanse
SELECT
    TRIM(ACCT_ID)           AS ACCT_ID,
    RPT_DT,
    TRIM(RPT_TYPE_CD)       AS RPT_TYPE_CD,
    TRIM(RPT_STATUS_CD)     AS RPT_STATUS_CD,
    RPT_AMT,
    TRIM(CURR_CD)           AS CURR_CD,
    POSTING_DT,
    COALESCE(REVERSAL_IND, 'N')         AS REVERSAL_IND,
    ORIG_RPT_ID,
    CREATE_DT,
    UPDATE_DT,
    'VisionPlus'                         AS SRC_SYSTEM_NM,
    CAST('${business_date}' AS DATE)     AS SRC_BUSINESS_DT
FROM query_001_extract_incremental
;


-- query_003_final
SELECT * FROM query_002_cleanse
;

-- dbutils.notebook.exit("query_003_final")

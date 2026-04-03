-- =============================================================================
-- FILE    : framework/sql/bronze/visionplus/brz_visionplus_ath3x_tgt.sql
-- LAYER   : BRONZE
-- SOURCE  : VisionPlus
-- TABLE   : ATH3X_TGT
-- STRATEGY: INC (watermark on UPDATE_DT)
-- =============================================================================

-- query_001_extract_incremental
SELECT
    ACCT_ID,
    CYCLE_DT,
    STMT_DT,
    AUTH_AMT,
    AUTH_TYPE_CD,
    MERCH_ID,
    MERCH_NM,
    MERCH_CITY,
    MERCH_COUNTRY_CD,
    TERMINAL_ID,
    POS_ENTRY_CD,
    DECLINE_RSN_CD,
    CREATE_DT,
    UPDATE_DT
FROM src_ath3x
WHERE UPDATE_DT > CAST('${watermark_value}' AS TIMESTAMP)
;

-- query_002_cleanse
SELECT
    TRIM(ACCT_ID)               AS ACCT_ID,
    CYCLE_DT,
    STMT_DT,
    AUTH_AMT,
    TRIM(AUTH_TYPE_CD)          AS AUTH_TYPE_CD,
    TRIM(MERCH_ID)              AS MERCH_ID,
    TRIM(MERCH_NM)              AS MERCH_NM,
    TRIM(MERCH_CITY)            AS MERCH_CITY,
    UPPER(TRIM(MERCH_COUNTRY_CD)) AS MERCH_COUNTRY_CD,
    TRIM(TERMINAL_ID)           AS TERMINAL_ID,
    TRIM(POS_ENTRY_CD)          AS POS_ENTRY_CD,
    TRIM(DECLINE_RSN_CD)        AS DECLINE_RSN_CD,
    CREATE_DT,
    UPDATE_DT,
    'VisionPlus'                AS SRC_SYSTEM_NM,
    CAST('${business_date}' AS DATE) AS SRC_BUSINESS_DT
FROM query_001_extract_incremental
;

-- query_003_final
SELECT * FROM query_002_cleanse
;

-- dbutils.notebook.exit("query_003_final")

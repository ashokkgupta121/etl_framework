-- =============================================================================
-- FILE    : framework/sql/bronze/visionplus/brz_visionplus_ath3d_tgt.sql
-- LAYER   : BRONZE
-- SOURCE  : VisionPlus
-- TABLE   : ATH3D_TGT
-- STRATEGY: FULL
-- DEPENDS : execution_order=2 (runs after wave-1 tables complete)
-- PURPOSE : Full refresh of ATH3D dimension table.
--           ATH3D is a reference/dimension table — full overwrite is safe
--           and preferred to avoid stale dimension values.
-- =============================================================================

-- query_001_extract
SELECT
    ACCT_ID,
    ACCT_TYPE_CD,
    ACCT_STATUS_CD,
    OPEN_DT,
    CLOSE_DT,
    CREDIT_LIMIT,
    CURR_BALANCE,
    AVAIL_CREDIT,
    STMT_CYCLE_DY,
    PMT_DUE_DY,
    INTEREST_RATE,
    CURR_CD,
    BRANCH_CD,
    RELATIONSHIP_MGR_ID,
    CREATE_DT,
    UPDATE_DT
FROM src_ath3d
;

-- query_002_cleanse_and_derive
SELECT
    TRIM(ACCT_ID)                           AS ACCT_ID,
    TRIM(ACCT_TYPE_CD)                      AS ACCT_TYPE_CD,
    TRIM(ACCT_STATUS_CD)                    AS ACCT_STATUS_CD,
    OPEN_DT,
    CLOSE_DT,
    CREDIT_LIMIT,
    CURR_BALANCE,
    AVAIL_CREDIT,

    -- Derived: utilisation ratio
    CASE
        WHEN CREDIT_LIMIT > 0
        THEN ROUND((CREDIT_LIMIT - AVAIL_CREDIT) / CREDIT_LIMIT * 100, 2)
        ELSE 0
    END                                     AS UTILISATION_PCT,

    STMT_CYCLE_DY,
    PMT_DUE_DY,
    INTEREST_RATE,
    UPPER(TRIM(CURR_CD))                    AS CURR_CD,
    TRIM(BRANCH_CD)                         AS BRANCH_CD,
    TRIM(RELATIONSHIP_MGR_ID)               AS RELATIONSHIP_MGR_ID,
    CREATE_DT,
    UPDATE_DT,

    -- Flag open vs closed
    CASE WHEN CLOSE_DT IS NULL THEN TRUE ELSE FALSE END AS IS_ACTIVE_ACCT,

    'VisionPlus'                            AS SRC_SYSTEM_NM,
    CAST('${business_date}' AS DATE)        AS SRC_BUSINESS_DT

FROM query_001_extract
;

-- query_003_final
SELECT * FROM query_002_cleanse_and_derive
;

-- dbutils.notebook.exit("query_003_final")

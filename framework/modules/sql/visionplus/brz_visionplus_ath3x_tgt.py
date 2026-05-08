class BrzVisionplusAth3xTgt:
    def __init__(self, params: dict):
        self.watermark_value = params["watermark_value"]
        self.business_date = params["business_date"]
        
        self.queries = [
            {
                "name": "query_001_extract_incremental",
                "sql": f"""
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
                    WHERE UPDATE_DT > CAST({self.watermark_value} AS TIMESTAMP)
                """
            },
            {
                "name": "query_002_cleanse",
                "sql": f"""
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
                        CAST('2026-01-01' AS DATE) AS SRC_BUSINESS_DT
                    FROM query_001_extract_incremental
                """
            },
            {
                "name": "query_003_final",
                "sql": """
                    SELECT * FROM query_002_cleanse
                """
            }
        ]
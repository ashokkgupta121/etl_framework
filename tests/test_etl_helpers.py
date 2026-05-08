# =============================================================================
# FILE    : tests/test_etl_helpers.py
# PURPOSE : Unit tests for etl_helpers — DeltaWriter write routing,
#           audit column injection, SQL multi-query parser, retry logic
# =============================================================================

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import date


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("etl_test_helpers")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# add_audit_columns
# ---------------------------------------------------------------------------

class TestAddAuditColumns:
    def test_all_audit_columns_added(self, spark):
        from framework.modules.etl_helpers import add_audit_columns
        df = spark.createDataFrame([(1, "A")], ["id", "name"])
        result = add_audit_columns(
            df               = df,
            batch_job_log_id = "1_20250724090000",
            job_config_id    = 5,
            business_date    = date(2025, 7, 24),
            load_strategy    = "FULL",
        )
        cols = result.columns
        assert "etl_batch_job_log_id" in cols
        assert "etl_job_config_id"    in cols
        assert "etl_business_date"    in cols
        assert "etl_load_ts"          in cols
        assert "etl_load_strategy"    in cols

    def test_original_columns_preserved(self, spark):
        from framework.modules.etl_helpers import add_audit_columns
        df = spark.createDataFrame([(99, "Z")], ["id", "name"])
        result = add_audit_columns(df, "log_1", 1, date(2025, 1, 1), "INC")
        assert "id"   in result.columns
        assert "name" in result.columns

    def test_audit_values_correct(self, spark):
        from framework.modules.etl_helpers import add_audit_columns
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_audit_columns(df, "batch_abc", 7, date(2025, 7, 24), "FULL")
        row = result.collect()[0]
        assert row["etl_batch_job_log_id"] == "batch_abc"
        assert row["etl_job_config_id"]    == 7
        assert row["etl_load_strategy"]    == "FULL"

    def test_business_date_cast_to_date(self, spark):
        from framework.modules.etl_helpers import add_audit_columns
        from pyspark.sql.types import DateType
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_audit_columns(df, "x", 1, date(2025, 3, 15), "FULL")
        field = [f for f in result.schema if f.name == "etl_business_date"][0]
        assert isinstance(field.dataType, DateType)


# ---------------------------------------------------------------------------
# TransformationEngine._parse_sql_queries
# ---------------------------------------------------------------------------

class TestParseSqlQueries:
    def _get_parser(self):
        from framework.modules.transformation_engine import TransformationEngine
        return TransformationEngine._parse_sql_queries

    def test_single_query_no_marker(self):
        parse = self._get_parser()
        sql   = "SELECT * FROM my_table"
        result = parse(sql)
        assert len(result) == 1
        assert "query_001_main" in result
        assert "SELECT * FROM my_table" in result["query_001_main"]

    def test_multi_query_markers_parsed(self):
        parse = self._get_parser()
        sql = """
-- query_001_extract
SELECT id FROM src;

-- query_002_transform
SELECT id, upper(name) AS name FROM query_001_extract;

-- query_003_final
SELECT * FROM query_002_transform;
"""
        result = parse(sql)
        assert len(result) == 3
        assert "query_001_extract"   in result
        assert "query_002_transform" in result
        assert "query_003_final"     in result

    def test_query_bodies_stripped(self):
        parse = self._get_parser()
        sql = "-- query_001_step\n  SELECT 1  \n"
        result = parse(sql)
        assert result["query_001_step"].strip() == "SELECT 1"

    def test_ordering_preserved(self):
        parse = self._get_parser()
        sql = "-- query_001_a\nSEL1;\n-- query_002_b\nSEL2;\n-- query_003_c\nSEL3;"
        result = parse(sql)
        keys = list(result.keys())
        assert keys == ["query_001_a", "query_002_b", "query_003_c"]


# ---------------------------------------------------------------------------
# TransformationEngine._interpolate_params
# ---------------------------------------------------------------------------

class TestInterpolateParams:
    def _get_fn(self):
        from framework.modules.transformation_engine import TransformationEngine
        return TransformationEngine._interpolate_params

    def test_single_param(self):
        fn = self._get_fn()
        result = fn("SELECT * FROM t WHERE dt = '${business_date}'",
                    {"business_date": "2025-07-24"})
        assert "2025-07-24" in result
        assert "${business_date}" not in result

    def test_multiple_params(self):
        fn = self._get_fn()
        sql    = "SELECT ${col} FROM ${tbl}"
        result = fn(sql, {"col": "id", "tbl": "orders"})
        assert result == "SELECT id FROM orders"

    def test_missing_param_left_as_is(self):
        fn = self._get_fn()
        sql    = "SELECT ${missing} FROM t"
        result = fn(sql, {})
        assert "${missing}" in result


# ---------------------------------------------------------------------------
# with_retry
# ---------------------------------------------------------------------------

class TestWithRetry:
    def test_success_first_attempt(self):
        from framework.modules.etl_helpers import with_retry
        mock_fn = MagicMock(return_value="ok")
        result  = with_retry(mock_fn, max_attempts=2, delay_seconds=0)
        assert result == "ok"
        assert mock_fn.call_count == 1

    def test_retries_on_failure_then_succeeds(self):
        from framework.modules.etl_helpers import with_retry
        call_count = {"n": 0}
        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise RuntimeError("transient error")
            return "recovered"
        result = with_retry(flaky, max_attempts=2, delay_seconds=0)
        assert result == "recovered"
        assert call_count["n"] == 2

    def test_raises_after_all_attempts(self):
        from framework.modules.etl_helpers import with_retry
        def always_fails():
            raise ValueError("always bad")
        with pytest.raises(ValueError, match="always bad"):
            with_retry(always_fails, max_attempts=2, delay_seconds=0)

    def test_no_reraise(self):
        from framework.modules.etl_helpers import with_retry
        def always_fails():
            raise RuntimeError("boom")
        result = with_retry(always_fails, max_attempts=1, delay_seconds=0, reraise=False)
        assert result is None


# ---------------------------------------------------------------------------
# check_min_row_count
# ---------------------------------------------------------------------------

class TestCheckMinRowCount:
    def test_no_minimum_always_passes(self, spark):
        from framework.modules.etl_helpers import check_min_row_count
        assert check_min_row_count(spark, "any_table", None) is True

    def test_zero_minimum_always_passes(self, spark):
        from framework.modules.etl_helpers import check_min_row_count
        assert check_min_row_count(spark, "any_table", 0) is True

    def test_passes_when_above_minimum(self, spark):
        from framework.modules.etl_helpers import check_min_row_count
        spark.sql("CREATE OR REPLACE TEMP VIEW _dq_test AS SELECT 1 AS id UNION ALL SELECT 2")
        assert check_min_row_count(spark, "_dq_test", 2) is True

    def test_fails_when_below_minimum(self, spark):
        from framework.modules.etl_helpers import check_min_row_count
        spark.sql("CREATE OR REPLACE TEMP VIEW _dq_test_small AS SELECT 1 AS id")
        assert check_min_row_count(spark, "_dq_test_small", 5) is False

# =============================================================================
# FILE    : tests/conftest.py
# PURPOSE : Shared pytest fixtures and sys.path setup for all test modules
# =============================================================================

import sys
import os
import pytest

# Make framework modules importable in test context
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "framework", "modules"))
sys.path.insert(0, os.path.join(ROOT, "framework"))
sys.path.insert(0, ROOT)


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession for all tests.
    Uses local[1] to avoid port conflicts in CI.
    """
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("etl_framework_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

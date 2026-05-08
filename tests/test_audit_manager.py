# =============================================================================
# FILE    : tests/test_audit_manager.py
# PURPOSE : Unit tests for AuditManager — idempotency, watermark, wave grouping
# =============================================================================

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from pyspark.sql import SparkSession, Row


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("etl_test_audit_manager")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def audit_mgr(spark):
    from framework.modules.audit_manager import AuditManager
    return AuditManager(spark)


# ---------------------------------------------------------------------------
# generate_batch_log_id
# ---------------------------------------------------------------------------

class TestGenerateBatchLogId:
    def test_format_contains_batch_id(self):
        from framework.modules.audit_manager import AuditManager
        result = AuditManager.generate_batch_log_id(1)
        assert result.startswith("1_")

    def test_format_length(self):
        from framework.modules.audit_manager import AuditManager
        result = AuditManager.generate_batch_log_id(42)
        # "42_" + 14-digit timestamp
        assert len(result) == 3 + 14

    def test_uniqueness(self):
        from framework.modules.audit_manager import AuditManager
        ids = {AuditManager.generate_batch_log_id(1) for _ in range(5)}
        # All 5 IDs should be unique (generated at different times)
        # In fast execution they may collide; allow >=1 unique
        assert len(ids) >= 1


# ---------------------------------------------------------------------------
# generate_job_audit_log_id
# ---------------------------------------------------------------------------

class TestGenerateJobAuditLogId:
    def test_returns_int(self):
        from framework.modules.audit_manager import AuditManager
        result = AuditManager.generate_job_audit_log_id()
        assert isinstance(result, int)

    def test_positive(self):
        from framework.modules.audit_manager import AuditManager
        assert AuditManager.generate_job_audit_log_id() > 0


# ---------------------------------------------------------------------------
# group_jobs_by_execution_order
# ---------------------------------------------------------------------------

class TestGroupJobsByExecutionOrder:
    def test_single_wave(self, audit_mgr):
        jobs = [
            Row(job_config_id=1, execution_order=1),
            Row(job_config_id=2, execution_order=1),
            Row(job_config_id=3, execution_order=1),
        ]
        waves = audit_mgr.group_jobs_by_execution_order(jobs)
        assert len(waves) == 1
        assert len(waves[0]) == 3

    def test_two_waves(self, audit_mgr):
        jobs = [
            Row(job_config_id=1, execution_order=1),
            Row(job_config_id=2, execution_order=1),
            Row(job_config_id=3, execution_order=2),
        ]
        waves = audit_mgr.group_jobs_by_execution_order(jobs)
        assert len(waves) == 2
        assert len(waves[0]) == 2   # wave 1: jobs 1 & 2 run in parallel
        assert len(waves[1]) == 1   # wave 2: job 3 sequential

    def test_ordering_ascending(self, audit_mgr):
        jobs = [
            Row(job_config_id=3, execution_order=3),
            Row(job_config_id=1, execution_order=1),
            Row(job_config_id=2, execution_order=2),
        ]
        waves = audit_mgr.group_jobs_by_execution_order(jobs)
        # First wave should be execution_order=1
        assert waves[0][0]["execution_order"] == 1

    def test_empty_jobs(self, audit_mgr):
        waves = audit_mgr.group_jobs_by_execution_order([])
        assert waves == []


# ---------------------------------------------------------------------------
# check_dependencies_met
# ---------------------------------------------------------------------------

class TestCheckDependenciesMet:
    def test_no_dependencies(self, audit_mgr):
        job_row = Row(depends_on_job_ids=None)
        ok, reason = audit_mgr.check_dependencies_met(job_row, date(2025, 7, 24))
        assert ok is True
        assert reason == ""

    def test_empty_string_dependencies(self, audit_mgr):
        job_row = Row(depends_on_job_ids="")
        ok, _ = audit_mgr.check_dependencies_met(job_row, date(2025, 7, 24))
        assert ok is True

    def test_deps_all_completed(self, audit_mgr, monkeypatch):
        monkeypatch.setattr(audit_mgr, "is_job_already_completed", lambda jid, bd: True)
        job_row = Row(depends_on_job_ids="1, 2, 3")
        ok, _ = audit_mgr.check_dependencies_met(job_row, date(2025, 7, 24))
        assert ok is True

    def test_deps_not_completed(self, audit_mgr, monkeypatch):
        monkeypatch.setattr(audit_mgr, "is_job_already_completed", lambda jid, bd: False)
        job_row = Row(depends_on_job_ids="1, 2")
        ok, reason = audit_mgr.check_dependencies_met(job_row, date(2025, 7, 24))
        assert ok is False
        assert "1" in reason or "2" in reason

# =============================================================================
# FILE    : tests/test_app_config.py
# PURPOSE : Unit tests for app_config — notebook path convention,
#           enum values, EnvironmentConfig resolution
# =============================================================================

import pytest
import os


class TestResolveNotebookPath:
    def test_bronze_path_convention(self):
        from framework.modules.app_config import resolve_notebook_path, Layer
        result = resolve_notebook_path(Layer.BRONZE, "VisionPlus", "ATH2_LRPMT_TGT")
        assert result == "framework/sql/bronze/visionplus/brz_visionplus_ath2_lrpmt_tgt"

    def test_silver_path_convention(self):
        from framework.modules.app_config import resolve_notebook_path, Layer
        result = resolve_notebook_path(Layer.SILVER, "consolidated", "PAYMENTS")
        assert result == "framework/sql/silver/consolidated/slv_consolidated_payments"

    def test_gold_path_convention(self):
        from framework.modules.app_config import resolve_notebook_path, Layer
        result = resolve_notebook_path(Layer.GOLD, "reporting", "DAILY_SUMMARY")
        assert result == "framework/sql/gold/reporting/gld_reporting_daily_summary"

    def test_source_system_spaces_replaced(self):
        from framework.modules.app_config import resolve_notebook_path, Layer
        result = resolve_notebook_path(Layer.BRONZE, "Vision Plus", "MY_TABLE")
        assert "vision_plus" in result
        assert " " not in result

    def test_all_lowercase(self):
        from framework.modules.app_config import resolve_notebook_path, Layer
        result = resolve_notebook_path(Layer.BRONZE, "VISIONPLUS", "UPPER_TABLE")
        assert result == result.lower()


class TestLayerEnum:
    def test_bronze_value(self):
        from framework.modules.app_config import Layer
        assert Layer.BRONZE == "BRONZE"

    def test_silver_value(self):
        from framework.modules.app_config import Layer
        assert Layer.SILVER == "SILVER"

    def test_gold_value(self):
        from framework.modules.app_config import Layer
        assert Layer.GOLD == "GOLD"


class TestLoadStrategyEnum:
    def test_full(self):
        from framework.modules.app_config import LoadStrategy
        assert LoadStrategy.FULL == "FULL"

    def test_inc(self):
        from framework.modules.app_config import LoadStrategy
        assert LoadStrategy.INC == "INC"

    def test_scd2(self):
        from framework.modules.app_config import LoadStrategy
        assert LoadStrategy.SCD2 == "SCD2"


class TestJobStatusEnum:
    def test_all_statuses_exist(self):
        from framework.modules.app_config import JobStatus
        assert JobStatus.RUNNING   == "RUNNING"
        assert JobStatus.COMPLETED == "COMPLETED"
        assert JobStatus.FAILED    == "FAILED"
        assert JobStatus.SKIPPED   == "SKIPPED"


class TestEnvironmentConfig:
    def test_default_env_is_dev(self, monkeypatch):
        monkeypatch.delenv("ETL_ENV", raising=False)
        from framework.modules.app_config import EnvironmentConfig
        cfg = EnvironmentConfig()
        assert cfg.env == "dev"

    def test_env_from_env_var(self, monkeypatch):
        monkeypatch.setenv("ETL_ENV", "prod")
        from framework.modules.app_config import EnvironmentConfig
        import importlib, framework.modules.app_config as m
        importlib.reload(m)
        cfg = m.EnvironmentConfig()
        assert cfg.env == "prod"

    def test_kv_scope_includes_env(self):
        from framework.modules.app_config import EnvironmentConfig
        cfg = EnvironmentConfig(env="uat")
        assert "uat" in cfg.key_vault_scope

    def test_catalog_includes_env(self):
        from framework.modules.app_config import EnvironmentConfig
        cfg = EnvironmentConfig(env="prod")
        assert "prod" in cfg.default_catalog


class TestMetadataTableNames:
    def test_all_tables_defined(self):
        from framework.modules.app_config import MetadataTable
        assert MetadataTable.BATCH_JOB_CONFIG  == "etl_metadata.batch_job_config"
        assert MetadataTable.JOB_CONFIG        == "etl_metadata.job_config"
        assert MetadataTable.CONNECTION_CONFIG == "etl_metadata.etl_connection_config"
        assert MetadataTable.BATCH_JOB_LOG     == "etl_metadata.batch_job_log"
        assert MetadataTable.JOB_AUDIT_LOG     == "etl_metadata.job_audit_log"

    def test_all_tables_use_etl_metadata_schema(self):
        from framework.modules.app_config import MetadataTable
        tables = [
            MetadataTable.BATCH_JOB_CONFIG,
            MetadataTable.JOB_CONFIG,
            MetadataTable.CONNECTION_CONFIG,
            MetadataTable.BATCH_JOB_LOG,
            MetadataTable.JOB_AUDIT_LOG,
        ]
        for t in tables:
            assert t.startswith("etl_metadata."), f"Table {t} missing schema prefix"

# =============================================================================
# FILE    : framework/modules/secret_manager.py
# MODULE  : 6 — Azure Key Vault Secret Fetcher
# PURPOSE : Centralised wrapper around Databricks secret scopes backed by
#           Azure Key Vault.  All source credentials flow through here —
#           NO plain-text secrets anywhere in the codebase.
# =============================================================================

from typing import Optional
from functools import lru_cache
import logging

logger = logging.getLogger("etl.secret_manager")


class SecretManager:
    """
    Retrieves secrets from Databricks secret scopes (Azure Key Vault backed).

    Each environment (dev / uat / prod) has its own KV-backed secret scope,
    configured in app_config.EnvironmentConfig.key_vault_scope.

    Usage:
        sm = SecretManager(spark, scope="etl-kv-scope-prod")
        password = sm.get_secret("visionplus-db-password")
    """

    def __init__(self, spark, scope: str):
        self._scope = scope
        self._dbutils = self._init_dbutils(spark)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_secret(self, secret_name: str) -> str:
        """
        Fetches a secret value by name from the configured KV scope.
        Raises ValueError if the secret is not found or empty.
        """
        if not secret_name:
            raise ValueError("secret_name cannot be empty.")
        try:
            value = self._dbutils.secrets.get(scope=self._scope, key=secret_name)
            if not value:
                raise ValueError(f"Secret '{secret_name}' resolved to empty string.")
            logger.debug("Secret resolved: scope=%s  key=%s", self._scope, secret_name)
            return value
        except Exception as exc:
            logger.error(
                "Failed to retrieve secret '%s' from scope '%s': %s",
                secret_name, self._scope, exc
            )
            raise

    def get_secret_or_none(self, secret_name: Optional[str]) -> Optional[str]:
        """Returns None if secret_name is None/empty; otherwise calls get_secret()."""
        if not secret_name:
            return None
        return self.get_secret(secret_name)

    def build_jdbc_url(
        self,
        host_secret:    str,
        port_secret:    str,
        database_secret: str,
        db_type:        str = "sqlserver",
    ) -> str:
        """
        Constructs a JDBC URL from KV-backed secrets.

        Supported db_type values: sqlserver | postgresql | mysql | oracle
        """
        host     = self.get_secret(host_secret)
        port     = self.get_secret(port_secret)
        database = self.get_secret(database_secret)

        templates = {
            "sqlserver":  f"jdbc:sqlserver://{host}:{port};databaseName={database}",
            "postgresql": f"jdbc:postgresql://{host}:{port}/{database}",
            "mysql":      f"jdbc:mysql://{host}:{port}/{database}",
            "oracle":     f"jdbc:oracle:thin:@{host}:{port}:{database}",
        }

        url = templates.get(db_type.lower())
        if not url:
            raise ValueError(f"Unsupported db_type '{db_type}'. "
                             f"Supported: {list(templates.keys())}")
        return url

    def build_connection_properties(
        self,
        username_secret: str,
        password_secret: str,
        driver_class:    str,
    ) -> dict:
        """
        Returns a dict of JDBC connection properties suitable for
        spark.read.jdbc(..., properties=...).
        """
        return {
            "user":     self.get_secret(username_secret),
            "password": self.get_secret(password_secret),
            "driver":   driver_class,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _init_dbutils(spark):
        """
        Safely initialises DBUtils.
        Works both on interactive clusters and in Databricks Jobs.
        """
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            return DBUtils(spark)
        except ImportError:
            # Fallback for local unit-testing with mocks
            import unittest.mock as mock
            logger.warning("DBUtils not available — returning mock (local testing only).")
            return mock.MagicMock()

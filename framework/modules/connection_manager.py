# =============================================================================
# FILE    : framework/modules/connection_manager.py
# MODULE  : 3 — External Source Connection Factory
# PURPOSE : Reads etl_connection_config metadata and builds live Spark readers
#           for each supported connection type (JDBC, ADLS, SFTP, Delta).
#           All credentials are fetched via SecretManager (Key Vault).
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import json
import logging

from secret_manager import SecretManager
from app_config import ConnectionType

logger = logging.getLogger("etl.connection_manager")


class ConnectionManager:
    """
    Factory that resolves connection metadata from etl_connection_config
    and provides typed read methods for each source type.

    Usage:
        cm = ConnectionManager(spark, secret_mgr)
        df = cm.read_jdbc(connection_row, query="SELECT * FROM orders")
    """

    def __init__(self, spark: SparkSession, secret_mgr: SecretManager):
        self._spark      = spark
        self._secret_mgr = secret_mgr

    # ------------------------------------------------------------------
    # Public read methods — called by the ingestion layer
    # ------------------------------------------------------------------

    def read_source(
        self,
        connection_row,         # Row from etl_connection_config
        query_or_table: str,    # SQL query string OR "schema.table"
        extra_options:  Optional[dict] = None,
        num_partitions: int = 8,
        partition_column: Optional[str] = None,
        lower_bound: Optional[str] = None,
        upper_bound: Optional[str] = None,
    ) -> DataFrame:
        """
        Dispatches to the correct reader based on connection_type.
        """
        conn_type = connection_row.connection_type.upper()

        if conn_type == ConnectionType.JDBC:
            return self.read_jdbc(
                connection_row, query_or_table, extra_options,
                num_partitions, partition_column, lower_bound, upper_bound
            )
        elif conn_type == ConnectionType.DELTA:
            return self.read_delta(query_or_table)
        elif conn_type == ConnectionType.ADLS:
            return self.read_adls(connection_row, query_or_table, extra_options)
        else:
            raise NotImplementedError(
                f"Connection type '{conn_type}' is not yet implemented. "
                f"Supported: JDBC, DELTA, ADLS."
            )

    def read_jdbc(
        self,
        connection_row,
        query_or_table:   str,
        extra_options:    Optional[dict] = None,
        num_partitions:   int = 8,
        partition_column: Optional[str] = None,
        lower_bound:      Optional[str] = None,
        upper_bound:      Optional[str] = None,
    ) -> DataFrame:
        """
        Reads a JDBC source using credentials from Key Vault.
        Supports parallel partitioned reads when partition_column is provided.
        """
        jdbc_url = self._secret_mgr.build_jdbc_url(
            host_secret     = connection_row.host_kv_secret,
            port_secret     = connection_row.port_kv_secret,
            database_secret = connection_row.database_kv_secret,
            db_type         = self._infer_db_type(connection_row.jdbc_driver_class or ""),
        )

        props = self._secret_mgr.build_connection_properties(
            username_secret = connection_row.username_kv_secret,
            password_secret = connection_row.password_kv_secret,
            driver_class    = connection_row.jdbc_driver_class,
        )
        
        print(props)

        # Merge extra options from config JSON
        if connection_row.extra_options_json:
            try:
                extra = json.loads(connection_row.extra_options_json)
                props.update(extra)
            except json.JSONDecodeError:
                logger.warning("Could not parse extra_options_json for connection %s",
                               connection_row.connection_id)

        if extra_options:
            props.update(extra_options)

        # Determine if query is a raw SQL expression or a table name
        is_query = query_or_table.strip().upper().startswith("SELECT")
        dbtable  = f"({query_or_table}) AS etl_src" if is_query else query_or_table
        print(jdbc_url)

        reader = self._spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", dbtable)

        # Parallel partitioned read (for large tables)
        if partition_column and lower_bound and upper_bound:
            reader = (
                reader
                .option("partitionColumn", partition_column)
                .option("lowerBound",      lower_bound)
                .option("upperBound",      upper_bound)
                .option("numPartitions",   str(num_partitions))
            )

        for k, v in props.items():
            reader = reader.option(k, v)

        df = reader.load()
        logger.info("JDBC read complete — connection: %s | rows read: %s",
                    connection_row.connection_id, df.count() if logger.isEnabledFor(logging.DEBUG) else "?")
        return df

    def read_delta(self, table_or_path: str) -> DataFrame:
        """
        Reads a Delta table by fully-qualified name or ADLS path.
        """
        if table_or_path.startswith("/") or table_or_path.startswith("abfss://"):
            return self._spark.read.format("delta").load(table_or_path)
        return self._spark.read.table(table_or_path)

    def read_adls(
        self,
        connection_row,
        file_path:     str,
        extra_options: Optional[dict] = None,
    ) -> DataFrame:
        """
        Reads files from Azure Data Lake Storage Gen2.
        Expects file_path to be an abfss:// URI or relative path.
        """
        storage_account = self._secret_mgr.get_secret_or_none(
            connection_row.host_kv_secret
        )
        sas_or_key = self._secret_mgr.get_secret_or_none(
            connection_row.password_kv_secret
        )

        if storage_account and sas_or_key:
            self._spark.conf.set(
                f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                sas_or_key,
            )

        fmt = (extra_options or {}).get("format", "parquet")
        reader = self._spark.read.format(fmt)

        if extra_options:
            for k, v in extra_options.items():
                if k != "format":
                    reader = reader.option(k, v)

        return reader.load(file_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_db_type(driver_class: str) -> str:
        driver_lower = driver_class.lower()
        if "sqlserver" in driver_lower:  return "sqlserver"
        if "postgresql" in driver_lower: return "postgresql"
        if "mysql"      in driver_lower: return "mysql"
        if "oracle"     in driver_lower: return "oracle"
        return "sqlserver"  # safe default

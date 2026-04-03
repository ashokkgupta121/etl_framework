# Databricks PySpark ETL Framework — Architecture & Developer Guide

## Overview

This framework provides a **metadata-driven, layered ETL pipeline** for Databricks, covering Bronze → Silver → Gold data layers. All table configurations, schedules, and load strategies are stored in metadata tables — no code changes are needed to onboard new tables.

---

## Repository Structure

```
databricks_etl_framework/
│
├── metadata_ddl/
│   └── 01_metadata_tables_ddl.sql        # Run once: creates all 5 metadata tables + seed data
│
├── framework/
│   ├── entry_batch_runner.py             # Databricks job entry point (called by Airflow)
│   │
│   ├── modules/                          # Core framework modules
│   │   ├── app_config.py                 # [M7] App-level constants, enums, env config
│   │   ├── session_manager.py            # [M2] SparkSession + session-level variables
│   │   ├── secret_manager.py             # [M6] Azure Key Vault secret fetcher
│   │   ├── connection_manager.py         # [M3] JDBC / ADLS / Delta source readers
│   │   ├── etl_logger.py                 # [M1] Writes to batch_job_log + job_audit_log
│   │   ├── audit_manager.py              # [M4] Idempotency checks, watermark, config fetch
│   │   ├── etl_helpers.py                # [M5] DeltaWriter, audit columns, DQ, retry
│   │   ├── transformation_engine.py      # [M9] SQL notebook executor, Delta writer
│   │   └── batch_orchestrator.py         # [M8] Top-level batch run coordinator
│   │
│   └── sql/                              # Transformation SQL notebooks
│       ├── bronze/
│       │   └── {source_system}/
│       │       └── brz_{source}_{table}.sql
│       ├── silver/
│       │   └── {domain}/
│       │       └── slv_{domain}_{table}.sql
│       └── gold/
│           └── {subject_area}/
│               └── gld_{subject}_{table}.sql
│
├── airflow/
│   ├── dag_etl_batch_runner.py           # Reusable DAG template
│   └── dag_visionplus_vp1.py             # Concrete DAG for VP-1 batch
│
├── tests/                                # Unit tests
└── docs/
    └── README.md                         # This file
```

---

## Metadata Tables (5 total)

### Configuration Tables (3 — one-time setup)

| Table | Purpose |
|---|---|
| `etl_metadata.batch_job_config` | One row per batch. Maps to an Airflow DAG. Defines schedule, parallelism, alerts. |
| `etl_metadata.job_config` | One row per table load. Source/target mapping, load strategy, notebook path, execution order. |
| `etl_metadata.etl_connection_config` | One row per source system. Connection type + Key Vault secret references. |

### Logging Tables (2 — written at runtime)

| Table | Purpose |
|---|---|
| `etl_metadata.batch_job_log` | One row per batch run per business_date. Tracks RUNNING → COMPLETED/FAILED/SKIPPED. |
| `etl_metadata.job_audit_log` | One row per table load. Row counts, watermark, error details, duration. |

---

## Module Map

| # | File | Responsibility |
|---|---|---|
| M1 | `etl_logger.py` | All writes to batch_job_log and job_audit_log |
| M2 | `session_manager.py` | SparkSession creation; session-level variables (business_date etc.) |
| M3 | `connection_manager.py` | Source system readers (JDBC, ADLS, Delta) |
| M4 | `audit_manager.py` | Idempotency checks, watermark state, config readers, dependency grouping |
| M5 | `etl_helpers.py` | DeltaWriter (FULL/INC/SCD1/SCD2/APPEND), audit columns, DQ checks, retry |
| M6 | `secret_manager.py` | Azure Key Vault secret fetcher via Databricks secret scopes |
| M7 | `app_config.py` | All enums, constants, env config, notebook path convention |
| M8 | `batch_orchestrator.py` | Top-level coordinator: waves of parallel jobs, lifecycle management |
| M9 | `transformation_engine.py` | SQL notebook executor, audit column injection, Delta write dispatch |

---

## SQL Notebook Naming Convention

```
framework/sql/{layer}/{source_system}/{prefix}_{source_system}_{table_name}.sql
```

| Layer  | Prefix |
|--------|--------|
| Bronze | `brz`  |
| Silver | `slv`  |
| Gold   | `gld`  |

**Examples:**
- `framework/sql/bronze/visionplus/brz_visionplus_ath2_lrpmt_tgt.sql`
- `framework/sql/silver/consolidated/slv_consolidated_payments.sql`
- `framework/sql/gold/reporting/gld_daily_payment_summary.sql`

### Multi-Query Convention

Each SQL file can contain multiple ordered queries separated by `-- query_NNN_step_name` markers:

```sql
-- query_001_extract
SELECT ... FROM src_table;

-- query_002_transform
SELECT ... FROM query_001_extract;

-- query_003_final
SELECT * FROM query_002_transform;

-- dbutils.notebook.exit("query_003_final")
```

- Each query result is registered as a Spark temp view named after its step key
- The **last query** produces the DataFrame written to the Delta target
- The notebook must exit with the final view name

---

## Execution Flow

```
Airflow DAG trigger (business_date, batch_job_config_id)
    └─▶ Databricks Job: entry_batch_runner.py
            └─▶ BatchOrchestrator.run()
                    ├─▶ IDEMPOTENCY CHECK (batch level)
                    │       Already COMPLETED? → SKIP entire batch
                    │
                    ├─▶ Fetch active job_configs, group into execution WAVES
                    │
                    ├─▶ Wave 1 [parallel, execution_order=1]
                    │       ├─▶ Job A: idempotency → transform → write → log
                    │       ├─▶ Job B: idempotency → transform → write → log
                    │       └─▶ Job C: idempotency → transform → write → log
                    │
                    └─▶ Wave 2 [sequential, execution_order=2]
                            └─▶ Job D: dep check → transform → write → log
```

---

## Idempotency / Skip Logic

### Batch level
Before any job starts, `AuditManager.is_batch_already_completed()` queries `batch_job_log`:
- If a `COMPLETED` record exists for `(batch_job_config_id, business_date)` → entire batch is **SKIPPED**
- This prevents duplicate loads on DAG re-runs or manual triggers

### Table level
Before each table load, `AuditManager.is_job_already_completed()` queries `job_audit_log`:
- If a `COMPLETED` record exists for `(job_config_id, business_date)` → that table is **SKIPPED**
- Other tables in the same batch continue normally

---

## Load Strategies

| Strategy | Behaviour |
|---|---|
| `FULL` | Overwrite entire target table on every run |
| `INC` | Delta MERGE using watermark column; only new/changed rows since last run |
| `SCD1` | Delta MERGE; update existing rows in place (no history) |
| `SCD2` | Expire old rows (`is_current=FALSE`), insert new active rows |
| `APPEND` | Insert-only; never update existing rows |

---

## Onboarding a New Table (Checklist)

1. **Register connection** (if new source): `INSERT INTO etl_metadata.etl_connection_config`
2. **Register batch** (if new batch): `INSERT INTO etl_metadata.batch_job_config`
3. **Register table**: `INSERT INTO etl_metadata.job_config` with:
   - `notebook_name` → path to SQL file (relative to `framework/sql/`)
   - `table_strategy` → FULL / INC / SCD1 / SCD2 / APPEND
   - `execution_order` → same value = parallel, higher value = later wave
   - `depends_on_job_ids` → comma-separated `job_config_id`s if hard dependencies exist
4. **Create SQL notebook**: `framework/sql/{layer}/{source}/{prefix}_{source}_{table}.sql`
5. **Create Airflow DAG** (if new batch): copy `dag_etl_batch_runner.py`, set `BATCH_CONFIG`
6. **Test**: run manually with `trigger_type=MANUAL`, verify `job_audit_log` entries

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| All table deps in metadata, not Airflow | Avoids DAG proliferation; one DAG = one batch regardless of table count |
| ThreadPoolExecutor for parallelism | Lighter than Databricks multi-task jobs for intra-batch parallelism |
| Delta MERGE for all non-FULL strategies | Idempotent by nature; safe for reruns |
| Watermark stored in `job_config` | Single source of truth; always consistent with last successful run |
| Session vars for cross-notebook params | Avoids widget re-declaration in every child notebook |
| Secrets only via Key Vault | No credentials in code, config files, or plain-text metadata |

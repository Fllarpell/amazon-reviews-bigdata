# Stage 1 summary

## Implemented

- Added template-compatible Stage 1 entrypoint: `bash scripts/stage1.sh`.
- Added stage sub-steps:
  - `scripts/data_collection.sh`
  - `scripts/data_storage.sh`
  - `scripts/data_ingestion.sh`
- Stage 1 Sqoop export uses Parquet + Snappy via `export/sqoop_parquet_export.sh`.
- Added `sql/` layer for reviewability:
  - `sql/create_tables.sql`
  - `sql/import_data.sql`
  - `sql/test_database.sql`
- Added template structure placeholders:
  - `data/`, `models/`, `notebooks/`, `output/`, `scripts/`, `sql/`.
- Updated docs and README to reflect Stage 1 flow and commands.

## Idempotency

- PostgreSQL load remains idempotent (`TRUNCATE + COPY` in `db/load_into_postgres.py`).
- HDFS imports remove destination directories before Sqoop import.
- HDFS parquet export targets are refreshed on each run.

## Validation notes

- `pylint` checks for Python pipeline files pass (`10.00/10`).
- `bash -n` syntax checks pass for all new shell scripts.
- End-to-end `scripts/stage1.sh` was executed locally and completed data collection; further steps depend on local PostgreSQL/Hadoop runtime availability.

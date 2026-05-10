# Stage 1 compliance matrix

Source of truth: `stages_instructions/BS - Stage I - Data collection and Ingestion - CodiMD.htm`.

| Stage 1 checklist item | Current state (before refactor) | Action in this refactor |
|---|---|---|
| Define schema for all tables | Implemented via migrations in `migrations/versions/001_baseline_amazon_appliances/deploy/*.sql` | Keep migrations, expose a stage-friendly SQL layer in `sql/` |
| Create all tables | Implemented in migration deploy scripts | Keep behavior, document and wire through stage scripts |
| Create all required constraints | Implemented (`fk_reviews_metadata`) | Keep behavior, verify remains idempotent |
| Load all data to all tables | Implemented via `etl/*` + `db/load_into_postgres.py` (`TRUNCATE + COPY`) | Keep behavior and make it callable from `scripts/stage1.sh` |
| Import all tables to HDFS serialized in a big-data format and compressed | Implemented as Parquet + Snappy in `export/sqoop_parquet_export.sh` | Keep Parquet + Snappy as project default |
| Put format-specific artifacts in `output/` | Optional for Parquet path | Keep `output/` for reports and stage deliverables |
| Write scripts to automate tasks | Implemented but not in course template layout (`bin/`, `export/`) | Add template-compatible `scripts/` entrypoints |
| Run `stage1.sh` to test stage | Missing | Add `scripts/stage1.sh` and wire full Stage 1 flow |
| Check quality of scripts using `pylint` | Present as `make lint`, but no stage-oriented target | Keep `make lint`, add Stage 1 verification step in docs |
| Summarize stage work in report | Partial docs exist | Add Stage 1 summary section in README/docs |

## Main compliance risks discovered

1. Stage format decision must stay consistent across scripts/docs (Parquet selected).
2. Missing stage entrypoint (`scripts/stage1.sh`) in expected location.
3. Missing committed project structure expected by the template (`scripts`, `sql`, `data`, `output`, `models`, `notebooks`).
4. Ensure `output/` remains available for submission artifacts.

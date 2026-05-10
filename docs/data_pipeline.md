# Data pipeline

See also: [setup_and_configuration.md](setup_and_configuration.md) · [database_and_migrations.md](database_and_migrations.md) · [hadoop_sqoop_and_hdfs.md](hadoop_sqoop_and_hdfs.md)

Short outline: [pipeline.txt](pipeline.txt)

## Flow

```text
Hugging Face JSONL → data/raw/*.jsonl
  → etl/fetch_review_dataset.py → data/staging/*.csv
  → etl/validate_staged_csv.py
  → db/load_into_postgres.py → public.metadata, public.reviews
  → export/sqoop_parquet_export.sh → HDFS Parquet + Snappy
  → export/hdfs_upload_staging.sh → HDFS CSV under …/staging/
  → optional hdfs_apply_replication.sh if HDFS_REPLICATION is set
```

Stage-1 entrypoint: **`scripts/stage1.sh`**.
Legacy wrappers: **`bin/run_pipeline.sh`** and root **`run_pipeline.sh`**.

## Fetch and stage

`etl/fetch_review_dataset.py` uses URLs in `config/constants.py`. Writes `data/raw/Appliances.jsonl` and `meta_Appliances.jsonl`, then `data/staging/metadata.csv` and `reviews.csv`.

Streaming HTTP download with `HTTP_TIMEOUT_SEC`. Skips re-download if the raw file exists and is non-empty unless `FORCE_RAW_REDOWNLOAD` is set. `JSONL_LINE_LIMIT` caps lines per JSONL when building CSV.

Reviews: keep rows whose `parent_asin` exists in metadata; dedupe on **`parent_asin` + `review_id`**. Field mapping in `lib/staging.py`.

## Validate

`etl/validate_staged_csv.py` checks file presence, `EXPECTED_MIN_*` line counts, and JSON key samples. Exits non-zero on failure so the orchestration stops before load.

## Load

`db/load_into_postgres.py` runs pending migrations, TRUNCATEs `metadata` and `reviews`, then COPY from staging. See [database_and_migrations.md](database_and_migrations.md).

## HDFS export

Sqoop JDBC → `HDFS_WAREHOUSE_BASE/reviews` and `/metadata` as Parquet with Snappy.
Staging CSV is copied to `/staging/`. Details: [hadoop_sqoop_and_hdfs.md](hadoop_sqoop_and_hdfs.md).

Local Docker uses a relative warehouse path under the HDFS user home; on a shared cluster set an absolute path such as `/user/team34/project/warehouse`.

## Re-runs

Re-run rebuilds staging from raw, re-validates, truncates and reloads Postgres, removes Sqoop target dirs in HDFS with `-skipTrash`, and overwrites staging CSV in HDFS with `put -f`.

## Logging

Python uses `lib/logutil` to stderr at `LOG_LEVEL`.

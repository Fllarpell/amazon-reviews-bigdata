# Amazon Appliances review pipeline

Formal team specification (PDF): [team34.pdf](team34.pdf) · Short overview: [docs/project_description.txt](docs/project_description.txt) · ETL outline: [docs/pipeline.txt](docs/pipeline.txt).

## Documentation (full)

| Document | Contents |
|----------|----------|
| [docs/setup_and_configuration.md](docs/setup_and_configuration.md) | Requirements, first run, `.env` / secrets, Makefile |
| [docs/data_pipeline.md](docs/data_pipeline.md) | End-to-end data flow, each stage, idempotency |
| [docs/database_and_migrations.md](docs/database_and_migrations.md) | PostgreSQL initdb, migrations ledger, scripts |
| [docs/hadoop_sqoop_and_hdfs.md](docs/hadoop_sqoop_and_hdfs.md) | Local Docker Hadoop vs cluster, Sqoop, HDFS scripts |

## Layout

| Path | Role |
|------|------|
| `scripts/stage1.sh` | Stage 1 entrypoint used by grader (`bash scripts/stage1.sh`) |
| `scripts/stage2.sh` | Stage 2 entrypoint used by grader (`bash scripts/stage2.sh`) |
| `scripts/stage3_prep.sh` | Data preparation pipeline before Stage 3 ML |
| `scripts/data_collection.sh` | Stage 1 data collection/validation step |
| `scripts/data_storage.sh` | Stage 1 PostgreSQL schema/load step |
| `scripts/data_ingestion.sh` | Stage 1 Sqoop/HDFS ingestion step |
| `scripts/stage2_spark_eda.py` | Stage 2 Spark SQL EDA runner (`q1..q3`) |
| `scripts/stage3_data_prep.py` | Stage 3 prep runner (cleaning, validation, train/test split) |
| `bin/run_pipeline.sh` | Ordered orchestration (legacy-compatible end-to-end wrapper) |
| `run_pipeline.sh` | Thin wrapper that calls `bin/run_pipeline.sh` |
| `config/constants.py` | Paths, URLs, thresholds, Postgres/HDFS env |
| `etl/` | **Extract + stage**: fetch JSONL, emit CSV; validate staging |
| `db/` | **Load + schema**: migrations, bulk load, verify, revert |
| `export/` | **Export**: Sqoop Parquet+Snappy (Stage 1), staging CSV → HDFS |
| `lib/` | Shared Python: logging, DB, migration runner |
| `migrations/versions/<id>/` | `deploy/*.sql`, `revert.sql`, `verify.sql`; ledger `pipeline.schema_migrations` |
| `reference/schema/` | Read-only DDL split (mirrors `deploy/`); not run by tools |
| `infra/docker/initdb/` | PostgreSQL container bootstrap (role + database) |
| `docs/` | Manuals (see **Documentation** above), `project_description.txt`, `pipeline.txt` |
| `team34.pdf` | Team / course specification (PDF) |
| `data/raw`, `data/staging` | Local datasets (gitignored) |
| `secrets/` | ` .psql.pass` (gitignored) |
| `sql/` | Stage-oriented SQL files (`create_tables.sql`, `import_data.sql`, `test_database.sql`) |
| `output/` | Submission artifacts and generated reports |

## Configuration

Copy [`.env.example`](.env.example) to `.env` and adjust `POSTGRES_PASSWORD`, `APP_DB_USER`, `APP_DB_NAME`, and matching `PGUSER` / `PGDATABASE` for host tools. Compose injects only `APP_DB_*` into the DB container for init.

## Postgres via Docker

```bash
cp .env.example .env
docker compose up -d
grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- > secrets/.psql.pass
```

Match `secrets/.psql.pass` to the `PGUSER` password (`POSTGRES_PASSWORD` from init).

## Commands

```bash
make install
make migrate
make verify-migrations
make pipeline
# CONFIRM=yes make revert-last-migration
```

Or run Stage 1 directly:

```bash
bash scripts/stage1.sh
```

Windows PowerShell (without `make`) local Docker helper:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_docker_stage1.ps1
```

Optional quick smoke run with smaller staged sample:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_docker_stage1.ps1 -JsonlLineLimit 10000
```

Or legacy wrapper: `bash run_pipeline.sh` / `bash bin/run_pipeline.sh`

Run Stage 2:

```bash
bash scripts/stage2.sh
```

Stage 2 runs through `beeline` and writes `output/hive_results.txt`, `output/q1.csv`, `output/q2.csv`, `output/q3.csv`, `output/q4.csv`.  
Optional environment variables for cluster runs: `HIVE_JDBC_URL`, `HIVE_USER`, `HIVE_PASSWORD`, `HIVE_DB_NAME`, `HIVE_DB_LOCATION`, `HDFS_WAREHOUSE_BASE`.

Clean generated artifacts before a fresh run:

```bash
bash scripts/clean_artifacts.sh
```

Also remove raw JSONL files:

```bash
bash scripts/clean_artifacts.sh --with-raw
```

Run Stage 3 data preparation:

```bash
bash scripts/stage3_prep.sh
```

### Full dataset

Appliances JSONL from Hugging Face is large (~2.1M review lines, ~1.1 GiB raw combined). To replace any truncated `data/raw/*.jsonl` and run end-to-end without `JSONL_LINE_LIMIT`:

```bash
rm -f data/raw/Appliances.jsonl data/raw/meta_Appliances.jsonl
# or: export FORCE_RAW_REDOWNLOAD=1
make pipeline-full
```

`make pipeline-full` loads `.env` then **unsets** `JSONL_LINE_LIMIT` so a value stored in `.env` does not cap the run. Ensure Postgres (and disk) can hold the load; `COPY` can take a while. On the Hadoop edge node, unset `SKIP_SQOOP` when you need Parquet export.

### Local full stack (Postgres + HDFS + Sqoop in Docker)

Requires **Docker** with the **Compose V2** plugin (`docker compose`). On **Apple Silicon**, Hadoop/Sqoop run as **`linux/amd64`** (emulation); on **Linux x86_64** the same compose files apply without extra flags.

```bash
cp .env.example .env
# secrets/.psql.pass must match DB password (see Postgres via Docker above)
make pipeline-all
```

`pipeline-all` runs `docker-up` (Citus), `hadoop-up`, builds the Sqoop image, then `bin/run_pipeline.sh` with **`USE_DOCKER_HADOOP=1`** (Sqoop and HDFS CSV upload use containers; Postgres stays on the host port from `.env`). For Postgres inside Docker on the same machine, the default **`SQOOP_PG_HOST=host.docker.internal`** is set in the export script; on Linux this relies on Docker’s **`host-gateway`** mapping (`docker run --add-host=host.docker.internal:host-gateway`). If JDBC from the Sqoop container cannot reach Postgres, set **`SQOOP_PG_HOST`** to your host LAN IP.

On the cluster, use **native** `hdfs` / `sqoop` with **`USE_DOCKER_HADOOP` unset** and set **`HDFS_WAREHOUSE_BASE`** to your team path (e.g. `/user/team34/project/warehouse`).

## Environment

| Variable | Meaning |
|----------|---------|
| `LOG_LEVEL` | Python logging level (default `INFO`) |
| `JSONL_LINE_LIMIT` | Cap lines per JSONL when staging (omit for full dataset; `make pipeline-full` clears it after sourcing `.env`) |
| `FORCE_RAW_REDOWNLOAD` | `1` / `true` / `yes` re-fetches raw |
| `HTTP_TIMEOUT_SEC` | Per-request timeout for streaming download (default `600`) |
| `EXPECTED_MIN_REVIEW_LINES` | Minimum raw review lines |
| `EXPECTED_MIN_METADATA_LINES` | Minimum raw metadata lines |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` | Connection |
| `APP_DB_USER`, `APP_DB_NAME` | Container init only |
| `POSTGRES_CONTAINER_NAME`, `POSTGRES_PORT` | Docker |
| `SKIP_SQOOP` | Skip Hadoop export |
| `SKIP_HDFS_STAGING` | Skip copying `data/staging/*.csv` into HDFS |
| `USE_DOCKER_HADOOP` | `1` / `true` / `yes`: Sqoop + HDFS staging uploads via Docker (`make pipeline-all` sets this) |
| `SQOOP_PG_HOST` | Hostname for JDBC from Sqoop container (default `host.docker.internal`) |
| `HDFS_WAREHOUSE_BASE`, `JDBC_URL` | Sqoop / HDFS paths and JDBC |
| `HDFS_REPLICATION` | If set (e.g. `2`), run `hdfs dfs -setrep -R -w` on `HDFS_WAREHOUSE_BASE` after exports (save space on shared clusters) |

## Lint

```bash
make lint
```

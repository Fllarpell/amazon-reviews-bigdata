# Setup and configuration

See also: [data_pipeline.md](data_pipeline.md) · [database_and_migrations.md](database_and_migrations.md) · [hadoop_sqoop_and_hdfs.md](hadoop_sqoop_and_hdfs.md)

## Requirements

| Tool | Use |
|------|-----|
| Python 3 | `etl/`, `db/`, `lib/` |
| pip + venv | `requirements.txt` |
| Bash | `bin/run_pipeline.sh`, `export/*.sh` |
| GNU Make | `Makefile` targets |
| Docker + Compose V2 | Citus, optional local Hadoop |
| Network | Hugging Face JSONL downloads |

On Windows, use **WSL2** with Docker Desktop.

## First run

```bash
cd /path/to/bigdata
python3 -m venv .venv
source .venv/bin/activate
make install
cp .env.example .env
docker compose up -d
grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- > secrets/.psql.pass
make migrate
```

`secrets/.psql.pass` must match the password for `PGUSER`. With default `.env.example`, `pipeline_app` uses `POSTGRES_PASSWORD`.

## Config files

**`.env`** — from `.env.example`. Groups: Postgres and Docker, ETL toggles, HDFS and Sqoop. Full variable list: [README.md](../README.md#environment).

**`secrets/.psql.pass`** — one line, password for `COPY` and Sqoop JDBC. Gitignored.

**`config/constants.py`** — default paths, dataset URLs, validation thresholds; overridable via env.

## Makefile targets

| Target | Action |
|--------|--------|
| `make install` | Install Python deps |
| `make lint` | pylint |
| `make secrets` | Ensure `secrets/.psql.pass` exists |
| `make migrate` | Apply pending migrations |
| `make verify-migrations` | Run `verify.sql` for applied versions |
| `make docker-up` / `docker-down` | Root `docker-compose.yml` |
| `make hadoop-up` / `hadoop-down` | `infra/hadoop/` stack |
| `make hadoop-build-sqoop` | Build `reviewhdfs-sqoop:local` |
| `make pipeline` | Full pipeline |
| `make pipeline-full` | Unsets `JSONL_LINE_LIMIT` after loading `.env` |
| `make pipeline-all` | Docker Postgres + Hadoop + `USE_DOCKER_HADOOP=1` |

## One-off env examples

```bash
export JSONL_LINE_LIMIT=10000 && make pipeline
export USE_DOCKER_HADOOP=1 && bash bin/run_pipeline.sh
export SKIP_SQOOP=1 && bash bin/run_pipeline.sh
```

`make pipeline-full` and `make pipeline-all` set their own env; see `Makefile`.

## Sanity checks

```bash
make verify-migrations
docker compose ps
docker compose -p reviewhdfs -f infra/hadoop/docker-compose.yml ps
bash infra/hadoop/wait_for_hdfs.sh
```

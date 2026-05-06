# Submission checklist (Moodle) and shared Hadoop cluster

## What to submit (course requirement)

Submit **both** on the assignment page:

1. **Public Git URL** (or URL the TA can open) to this repository.
2. **Absolute path** on the university host **`hadoop-01.uni.innopolis.ru`** to the **same** clone of the repo (reachable by your team user, e.g. `teamN`).

Keep the remote and the cluster clone in sync: the TA will compare Git history and structure with what they run on the cluster.

## How the TA verifies (summary)

- Runs pipeline stages **one by one**, **multiple times**, under your team account.
- Checks **YARN / MapReduce / other job history** on the cluster.
- Opens your **public** final-stage dashboard and re-runs charts (only **one** public dashboard for grading).
- Reviews **documentation** in the Git repository.

After the deadline you may lose cluster access; document anything a grader needs in the repo.

## Cluster capacity and HDFS space (Innopolis)

- **YARN queue load:** open the scheduler, e.g. `http://hadoop-03.uni.innopolis.ru:8088/cluster/scheduler` and check **`root.teams`**. If usage is very high, your job may wait; coordinate with other teams if needed.
- **Replication:** default replication multiplies storage. After you own data under your home (e.g. `/user/teamN/...`), you can lower replication if policy allows:
  - `hdfs dfs -setrep -w 2 /user/teamN` (example: wait for completion; use a value your course allows).
- **Trash:** deletes in HDFS go under **`.Trash`** under your user directory. Periodically empty it to free space, e.g. remove your trash folder when you no longer need deleted data.

This repo supports an optional env var **`HDFS_REPLICATION`** (e.g. `2`): after Sqoop and after staging CSV upload, scripts run `hdfs dfs -setrep -R -w <n>` on **`HDFS_WAREHOUSE_BASE`** when set. Omit it to keep the cluster default.

## HDFS paths on the cluster

Point **`HDFS_WAREHOUSE_BASE`** in `.env` at the directory **under your team account** where artifacts should live, for example:

`/user/team0/project/warehouse`

Use the same layout as locally (`reviews`, `metadata`, `staging` under that base) so scripts match the TA’s expectations.

## Cluster runbook (Innopolis)

Local Docker validates the repo; it does **not** replace a run on the course YARN/HDFS.

### 1. Clone and environment

- SSH as your team user to **`hadoop-01.uni.innopolis.ru`** or the host your course specifies.
- Clone into the directory you will list in Moodle; **`git pull`** before submission.
- `python3 -m venv .venv && source .venv/bin/activate && make install`
- Confirm **`hdfs`** and **`sqoop`** are on **`PATH`**.

### 2. `.env` on the cluster

Maintain a cluster-specific `.env`:

| Variable | Cluster |
|----------|---------|
| **`USE_DOCKER_HADOOP`** | Unset. Use native `hdfs` and `sqoop`. |
| **`HDFS_WAREHOUSE_BASE`** | Absolute path under your user, e.g. `/user/teamN/project/warehouse`. |
| **`PGHOST`**, **`PGPORT`**, **`PGUSER`**, **`PGDATABASE`** | Reachable Postgres for the project. Sqoop on the edge node must open JDBC to this host. |
| **`secrets/.psql.pass`** | One line: password for **`PGUSER`**. |
| **`HDFS_REPLICATION`** | Optional, e.g. `2`, if scripts should call `setrep` after writes. |
| **`JSONL_LINE_LIMIT`** | Omit for full data when matching TA runs. |

If Postgres runs only on a laptop and Sqoop runs on the cluster, JDBC from the cluster to the laptop usually fails. Use a DB reachable from the cluster or follow the course’s deployment model.

### 3. Migrations and stages

- `make migrate` and optionally `make verify-migrations`.
- Run stepwise and repeat like the TA:
  1. `python etl/fetch_review_dataset.py`
  2. `python etl/validate_staged_csv.py`
  3. `python db/load_into_postgres.py`
  4. `bash export/sqoop_parquet_export.sh`
  5. `bash export/hdfs_upload_staging.sh`

Or once: **`bash bin/run_pipeline.sh`** with `.env` loaded.

### 4. Check HDFS

```bash
hdfs dfs -ls -R "/user/teamN/project/warehouse"
```

Expect **`reviews/`**, **`metadata/`** with Parquet and Kite **`.metadata`**, and **`staging/`** with both CSV files.

### 5. Capacity and trash

Watch **`root.teams`** before large jobs. Remove **`/user/teamN/.Trash`** when the course allows and you need space.

### 6. Run `make pipeline-all` twice on a Mac?

**Not required** for cluster grading. A second local run is an optional smoke test; grading depends on Innopolis with native Hadoop and your **`HDFS_WAREHOUSE_BASE`**.

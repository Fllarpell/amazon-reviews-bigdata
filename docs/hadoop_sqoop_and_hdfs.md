# Hadoop, Sqoop, and HDFS

See also: [setup_and_configuration.md](setup_and_configuration.md) · [data_pipeline.md](data_pipeline.md) · [submission_and_cluster.md](submission_and_cluster.md)

## Modes

### A. Local Docker

Stack: **`infra/hadoop/docker-compose.yml`**, Compose project **`reviewhdfs`**.

- NameNode, DataNode, ResourceManager, NodeManager; image **`apache/hadoop:3`**, platform **`linux/amd64`** for ARM Macs.
- Readiness: **`infra/hadoop/wait_for_hdfs.sh`**.

With **`USE_DOCKER_HADOOP=1`** as in **`make pipeline-all`**:

- Sqoop runs in **`reviewhdfs-sqoop:local`** from **`infra/hadoop/sqoop/Dockerfile`**: Sqoop 1.4.7, PostgreSQL JDBC, `commons-lang`, Temurin JDK 8.
- **`hdfs`** for CSV upload: ephemeral **`apache/hadoop:3`** containers on **`reviewhdfs_default`**.

Default JDBC host **`SQOOP_PG_HOST=host.docker.internal`** with **`host.docker.internal:host-gateway`** in `docker run`. On Linux, set **`SQOOP_PG_HOST`** to the host IP if needed.

### B. Shared cluster

Unset **`USE_DOCKER_HADOOP`**. **`hdfs`** and **`sqoop`** on **`PATH`**; cluster config via the usual env e.g. **`HADOOP_CONF_DIR`**.

Set **`HDFS_WAREHOUSE_BASE`** to an absolute path, e.g. **`/user/teamN/project/warehouse`**.

Layout under that prefix:

- **`reviews/`** — Sqoop Parquet
- **`metadata/`** — Sqoop Parquet
- **`staging/`** — **`reviews.csv`**, **`metadata.csv`**

## `export/` scripts

### `sqoop_parquet_export.sh`

- Exits if **`SKIP_SQOOP=1`**.
- Uses Docker when **`USE_DOCKER_HADOOP=1`** or when **`sqoop`** is missing.
- Deletes **`${HDFS_WAREHOUSE_BASE}/reviews`** and **`/metadata`** before import with **`-skipTrash`**.
- Import: **`--as-parquetfile`**, Snappy, **`-m 1`**.
- Container sets **`HADOOP_USER_NAME=hadoop`** so local HDFS allows writes.
- Calls **`hdfs_apply_replication.sh`** after success if **`HDFS_REPLICATION`** is set.
- **`JDBC_URL`** overrides the full URL; else built from **`SQOOP_PG_HOST`**, port **5432**, **`PGDATABASE`**.

### `hdfs_upload_staging.sh`

- Puts **`data/staging/reviews.csv`** and **`metadata.csv`** into **`${HDFS_WAREHOUSE_BASE}/staging/`**.
- Skipped when **`SKIP_HDFS_STAGING=1`**.
- Docker vs native follows **`USE_DOCKER_HADOOP`** or presence of **`hdfs`**.
- Runs **`hdfs_apply_replication.sh`** at the end when configured.

### `hdfs_apply_replication.sh`

- No-op if **`HDFS_REPLICATION`** is unset.
- Else **`hdfs dfs -setrep -R -w <n> <HDFS_WAREHOUSE_BASE>`**. See [submission_and_cluster.md](submission_and_cluster.md).

## Sqoop image

**`infra/hadoop/sqoop/Dockerfile`**: base **`apache/hadoop:3`**, Sqoop 1.4.7 tarball, PostgreSQL JAR, **`commons-lang-2.6`**, Temurin 8 JDK for Sqoop codegen.

Build: **`make hadoop-build-sqoop`**.

## Checks

```bash
docker compose -p reviewhdfs -f infra/hadoop/docker-compose.yml exec -T namenode \
  hdfs dfs -ls -R project/warehouse
```

NameNode UI is typically **http://localhost:9870** if the port is published.

On the cluster: **`hdfs dfs -ls -R /user/teamN/project/warehouse`**.

## Sqoop warnings

Missing HBase, HCatalog, Accumulo, and hints about **`--direct`** on Postgres are normal for this image and do not block Parquet import for this project.

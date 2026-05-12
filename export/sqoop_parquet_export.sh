#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HADOOP_DIR="${ROOT}/infra/hadoop"
HADOOP_COMPOSE=(docker compose -p reviewhdfs -f "${HADOOP_DIR}/docker-compose.yml")
SQOOP_IMAGE="${SQOOP_DOCKER_IMAGE:-reviewhdfs-sqoop:local}"
HADOOP_NET="${HADOOP_DOCKER_NETWORK:-reviewhdfs_default}"
SQOOP_PG_HOST="${SQOOP_PG_HOST:-host.docker.internal}"

if [[ "${SKIP_SQOOP:-}" =~ ^(1|true|yes)$ ]]; then
  echo "SKIP_SQOOP set, skipping Sqoop"
  exit 0
fi

use_docker=false
if [[ "${USE_DOCKER_HADOOP:-}" =~ ^(1|true|yes)$ ]]; then
  use_docker=true
elif ! command -v sqoop >/dev/null 2>&1; then
  use_docker=true
fi

PASS_FILE="${ROOT}/secrets/.psql.pass"
if [[ ! -f "${PASS_FILE}" ]]; then
  echo "Password file not found: ${PASS_FILE}" >&2
  exit 1
fi
read -r PASS < "${PASS_FILE}" || true
if [[ -z "${PASS}" ]]; then
  echo "Password file is empty: ${PASS_FILE}" >&2
  exit 1
fi
DB_USER="${PGUSER:-pipeline_app}"
DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-review_analytics}"
WAREHOUSE="${HDFS_WAREHOUSE_BASE:-project/warehouse}"

if [[ "$use_docker" == true ]]; then
  if ! docker network inspect "${HADOOP_NET}" >/dev/null 2>&1; then
    echo "Docker network ${HADOOP_NET} not found. Run: make hadoop-up && make hadoop-build-sqoop" >&2
    exit 1
  fi
  JDBC_URL="${JDBC_URL:-jdbc:postgresql://${SQOOP_PG_HOST}:${DB_PORT}/${DB_NAME}}"
  echo "Sqoop via Docker (${SQOOP_IMAGE}), JDBC ${JDBC_URL}"
  echo "clearing HDFS targets under ${WAREHOUSE}"
  "${HADOOP_COMPOSE[@]}" exec -T namenode hdfs dfs -rm -r -skipTrash "${WAREHOUSE}/reviews" 2>/dev/null || true
  "${HADOOP_COMPOSE[@]}" exec -T namenode hdfs dfs -rm -r -skipTrash "${WAREHOUSE}/metadata" 2>/dev/null || true
  run_sqoop() {
    docker run --rm \
      --platform linux/amd64 \
      --network "${HADOOP_NET}" \
      --add-host=host.docker.internal:host-gateway \
      --env-file "${HADOOP_DIR}/hadoop.env" \
      -e "HADOOP_USER_NAME=hadoop" \
      -e "HADOOP_MAPRED_HOME=//opt/hadoop" \
      -e "HADOOP_HOME=//opt/hadoop" \
      -e "SQOOP_HOME=//opt/sqoop" \
      "${SQOOP_IMAGE}" \
      sqoop "$@"
  }
  echo "sqoop import reviews -> ${WAREHOUSE}/reviews"
  run_sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${PASS}" \
    --table reviews \
    --target-dir "${WAREHOUSE}/reviews" \
    --as-parquetfile \
    --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
    -m 1
  echo "sqoop import metadata -> ${WAREHOUSE}/metadata"
  run_sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${PASS}" \
    --table metadata \
    --target-dir "${WAREHOUSE}/metadata" \
    --as-parquetfile \
    --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
    -m 1
  bash "${ROOT}/export/hdfs_apply_replication.sh"
  echo "sqoop export finished"
  exit 0
fi

if ! command -v sqoop >/dev/null 2>&1; then
  echo "sqoop not found; start Hadoop (make hadoop-up), build image (make hadoop-build-sqoop), set USE_DOCKER_HADOOP=1"
  exit 1
fi

JDBC_URL="${JDBC_URL:-jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}}"

echo "clearing HDFS targets under ${WAREHOUSE}"
hdfs dfs -rm -r -skipTrash "${WAREHOUSE}/reviews" 2>/dev/null || true
hdfs dfs -rm -r -skipTrash "${WAREHOUSE}/metadata" 2>/dev/null || true

echo "sqoop import reviews -> ${WAREHOUSE}/reviews"
sqoop import \
  --connect "${JDBC_URL}" \
  --username "${DB_USER}" \
  --password "${PASS}" \
  --table reviews \
  --target-dir "${WAREHOUSE}/reviews" \
  --as-parquetfile \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  -m 1

echo "sqoop import metadata -> ${WAREHOUSE}/metadata"
sqoop import \
  --connect "${JDBC_URL}" \
  --username "${DB_USER}" \
  --password "${PASS}" \
  --table metadata \
  --target-dir "${WAREHOUSE}/metadata" \
  --as-parquetfile \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  -m 1

bash "${ROOT}/export/hdfs_apply_replication.sh"
echo "sqoop export finished"

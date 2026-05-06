#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HADOOP_DIR="${ROOT}/infra/hadoop"
HADOOP_COMPOSE=(docker compose -p reviewhdfs -f "${HADOOP_DIR}/docker-compose.yml")
HADOOP_NET="${HADOOP_DOCKER_NETWORK:-reviewhdfs_default}"
WAREHOUSE="${HDFS_WAREHOUSE_BASE:-project/warehouse}"
STAGING_LOCAL="${ROOT}/data/staging"
REVIEWS_CSV="${STAGING_LOCAL}/reviews.csv"
META_CSV="${STAGING_LOCAL}/metadata.csv"

if [[ "${SKIP_HDFS_STAGING:-}" =~ ^(1|true|yes)$ ]]; then
  echo "SKIP_HDFS_STAGING set, skipping CSV upload to HDFS"
  exit 0
fi

use_docker=false
if [[ "${USE_DOCKER_HADOOP:-}" =~ ^(1|true|yes)$ ]]; then
  use_docker=true
elif ! command -v hdfs >/dev/null 2>&1; then
  use_docker=true
fi

dest_base="${WAREHOUSE%/}/staging"

upload_docker() {
  if ! docker network inspect "${HADOOP_NET}" >/dev/null 2>&1; then
    echo "Docker network ${HADOOP_NET} not found. Run: make hadoop-up" >&2
    exit 1
  fi
  [[ -f "${REVIEWS_CSV}" && -f "${META_CSV}" ]] || {
    echo "missing ${REVIEWS_CSV} or ${META_CSV}; run ETL first" >&2
    exit 1
  }
  echo "HDFS: upload staging CSV -> ${dest_base}/ (via Docker)"
  docker run --rm \
    --platform linux/amd64 \
    --network "${HADOOP_NET}" \
    --env-file "${HADOOP_DIR}/hadoop.env" \
    -e "HADOOP_USER_NAME=hadoop" \
    -e "HADOOP_HOME=/opt/hadoop" \
    -v "${STAGING_LOCAL}:/staging:ro" \
    apache/hadoop:3 \
    bash -lc "hdfs dfs -mkdir -p '${dest_base}' && hdfs dfs -put -f /staging/reviews.csv '${dest_base}/' && hdfs dfs -put -f /staging/metadata.csv '${dest_base}/'"
}

upload_native() {
  [[ -f "${REVIEWS_CSV}" && -f "${META_CSV}" ]] || {
    echo "missing staging CSV; run ETL first" >&2
    exit 1
  }
  echo "HDFS: upload staging CSV -> ${dest_base}/"
  hdfs dfs -mkdir -p "${dest_base}"
  hdfs dfs -put -f "${REVIEWS_CSV}" "${dest_base}/"
  hdfs dfs -put -f "${META_CSV}" "${dest_base}/"
}

if [[ "$use_docker" == true ]]; then
  upload_docker
else
  upload_native
fi

bash "${ROOT}/export/hdfs_apply_replication.sh"
echo "HDFS staging CSV upload finished"

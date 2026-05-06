#!/usr/bin/env bash
# If HDFS_REPLICATION is set (e.g. 2 on a shared university cluster), apply recursively under HDFS_WAREHOUSE_BASE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HADOOP_DIR="${ROOT}/infra/hadoop"
HADOOP_NET="${HADOOP_DOCKER_NETWORK:-reviewhdfs_default}"
WAREHOUSE="${HDFS_WAREHOUSE_BASE:-project/warehouse}"

rep="${HDFS_REPLICATION:-}"
if [[ -z "${rep}" ]]; then
  exit 0
fi
if ! [[ "${rep}" =~ ^[1-9][0-9]*$ ]]; then
  echo "HDFS_REPLICATION must be a positive integer, got: ${rep}" >&2
  exit 1
fi

use_docker=false
if [[ "${USE_DOCKER_HADOOP:-}" =~ ^(1|true|yes)$ ]]; then
  use_docker=true
elif ! command -v hdfs >/dev/null 2>&1; then
  use_docker=true
fi

if [[ "$use_docker" == true ]]; then
  if ! docker network inspect "${HADOOP_NET}" >/dev/null 2>&1; then
    echo "HDFS_REPLICATION=${rep} set but Docker network ${HADOOP_NET} missing; skip setrep (run make hadoop-up) or use native hdfs" >&2
    exit 0
  fi
  echo "HDFS: setrep -R -w ${rep} ${WAREHOUSE}/ (via Docker)"
  docker run --rm \
    --platform linux/amd64 \
    --network "${HADOOP_NET}" \
    --env-file "${HADOOP_DIR}/hadoop.env" \
    -e "HADOOP_USER_NAME=hadoop" \
    -e "HADOOP_HOME=/opt/hadoop" \
    apache/hadoop:3 \
    bash -lc "hdfs dfs -setrep -R -w '${rep}' '${WAREHOUSE}'"
else
  echo "HDFS: setrep -R -w ${rep} ${WAREHOUSE}/"
  hdfs dfs -setrep -R -w "${rep}" "${WAREHOUSE}"
fi

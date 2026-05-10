#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"

echo "Stage 1: data collection"
bash "${ROOT}/scripts/data_collection.sh"

echo "Stage 1: data storage (PostgreSQL)"
bash "${ROOT}/scripts/data_storage.sh"

echo "Stage 1: data ingestion (Sqoop -> HDFS)"
bash "${ROOT}/scripts/data_ingestion.sh"

echo "Stage 1 completed"

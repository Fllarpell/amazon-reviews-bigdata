#!/usr/bin/env bash
# Stage I bulk path: fetch JSONL, validate staging, load Postgres, export Parquet, upload CSV to HDFS.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${ROOT}"
cd "${ROOT}"
source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"
"${PYTHON_CMD[@]}" "${ROOT}/etl/fetch_review_dataset.py"
"${PYTHON_CMD[@]}" "${ROOT}/etl/validate_staged_csv.py"
"${PYTHON_CMD[@]}" "${ROOT}/db/load_into_postgres.py"
bash "${ROOT}/export/sqoop_parquet_export.sh"
bash "${ROOT}/export/hdfs_upload_staging.sh"

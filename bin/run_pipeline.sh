#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${ROOT}"
cd "${ROOT}"
if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi
if [[ -z "${PYTHON:-}" && -x "${ROOT}/.venv/bin/python" ]]; then
  PYTHON="${ROOT}/.venv/bin/python"
fi
PYTHON="${PYTHON:-python3}"
"${PYTHON}" "${ROOT}/etl/fetch_review_dataset.py"
"${PYTHON}" "${ROOT}/etl/validate_staged_csv.py"
"${PYTHON}" "${ROOT}/db/load_into_postgres.py"
bash "${ROOT}/export/sqoop_parquet_export.sh"
bash "${ROOT}/export/hdfs_upload_staging.sh"

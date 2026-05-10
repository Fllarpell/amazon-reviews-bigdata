#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
resolve_python_cmd "${ROOT}"

"${PYTHON_CMD[@]}" "${ROOT}/etl/fetch_review_dataset.py"
"${PYTHON_CMD[@]}" "${ROOT}/etl/validate_staged_csv.py"

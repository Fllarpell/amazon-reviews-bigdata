#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

PYTHON_CMD=()
if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_CMD=("${PYTHON}")
elif [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PYTHON_CMD=("${ROOT}/.venv/bin/python")
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=(python3)
elif command -v py >/dev/null 2>&1; then
  PYTHON_CMD=(py -3)
elif command -v py.exe >/dev/null 2>&1; then
  PYTHON_CMD=(py.exe -3)
else
  echo "No Python interpreter found (tried .venv/bin/python, python3, py)." >&2
  exit 1
fi

"${PYTHON_CMD[@]}" "${ROOT}/etl/fetch_review_dataset.py"
"${PYTHON_CMD[@]}" "${ROOT}/etl/validate_staged_csv.py"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

PYTHON_CMD=()
if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_CMD=("${PYTHON}")
elif [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PYTHON_CMD=("${ROOT}/.venv/bin/python")
elif command -v py >/dev/null 2>&1; then
  PYTHON_CMD=(py -3)
elif command -v py.exe >/dev/null 2>&1; then
  PYTHON_CMD=(py.exe -3)
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=(python3)
else
  echo "No Python interpreter found (tried .venv/bin/python, python3, py)." >&2
  exit 1
fi

HIVE_DB_NAME="${HIVE_DB_NAME:-teamx_projectdb}"
HIVE_DB_LOCATION="${HIVE_DB_LOCATION:-project/hive/warehouse/teamx_projectdb}"
SCRIPT_PATH="${ROOT}/scripts/stage2_spark_eda.py"

if [[ "${PYTHON_CMD[0]}" =~ ^(py|py\.exe|python\.exe)$ ]]; then
  if command -v cygpath >/dev/null 2>&1; then
    SCRIPT_PATH="$(cygpath -w "${SCRIPT_PATH}")"
  elif [[ "${SCRIPT_PATH}" == /mnt/* ]]; then
    drive_letter="$(echo "${SCRIPT_PATH}" | cut -d'/' -f3 | tr '[:lower:]' '[:upper:]')"
    suffix="$(echo "${SCRIPT_PATH}" | cut -d'/' -f4- | sed 's#/#\\\\#g')"
    SCRIPT_PATH="${drive_letter}:\\${suffix}"
  fi
fi

echo "Stage 2: Hive DDL + Spark SQL EDA"
"${PYTHON_CMD[@]}" "${SCRIPT_PATH}" \
  --mode all \
  --hive-db-name "${HIVE_DB_NAME}" \
  --hive-db-location "${HIVE_DB_LOCATION}"

for csv_file in output/q1.csv output/q2.csv output/q3.csv; do
  if [[ ! -s "${csv_file}" ]]; then
    echo "Missing or empty ${csv_file}" >&2
    exit 1
  fi
done

if [[ ! -s output/hive_results.txt ]]; then
  echo "Missing or empty output/hive_results.txt" >&2
  exit 1
fi

echo "Stage 2 completed"

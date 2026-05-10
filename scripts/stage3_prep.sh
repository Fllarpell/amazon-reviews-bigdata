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
  echo "No Python interpreter found (tried .venv/bin/python, py, python3)." >&2
  exit 1
fi

SCRIPT_PATH="${ROOT}/scripts/stage3_data_prep.py"
if [[ "${PYTHON_CMD[0]}" =~ ^(py|py\.exe|python\.exe)$ ]]; then
  if command -v cygpath >/dev/null 2>&1; then
    SCRIPT_PATH="$(cygpath -w "${SCRIPT_PATH}")"
  elif [[ "${SCRIPT_PATH}" == /mnt/* ]]; then
    drive_letter="$(echo "${SCRIPT_PATH}" | cut -d'/' -f3 | tr '[:lower:]' '[:upper:]')"
    suffix="$(echo "${SCRIPT_PATH}" | cut -d'/' -f4- | sed 's#/#\\\\#g')"
    SCRIPT_PATH="${drive_letter}:\\${suffix}"
  fi
fi

echo "Stage 3 prep: data profiling + cleaning + train/test split"
"${PYTHON_CMD[@]}" "${SCRIPT_PATH}"

for artifact in \
  "data/processed/ml_dataset.csv" \
  "data/train.json" \
  "data/test.json" \
  "output/data_profile_before.csv" \
  "output/data_profile_after.csv" \
  "output/data_quality_report.txt"; do
  if [[ ! -s "${artifact}" ]]; then
    echo "Missing or empty artifact: ${artifact}" >&2
    exit 1
  fi
done

echo "Stage 3 prep completed"

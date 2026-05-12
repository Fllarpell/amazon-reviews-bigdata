#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"

resolve_python_cmd "${ROOT}"

SCRIPT_PATH="${ROOT}/scripts/legacy/stage3_data_prep.py"
SCRIPT_PATH="$(python_script_path_for_platform "${SCRIPT_PATH}")"

echo "Stage 3 local prep (legacy helper): data profiling + cleaning + train/test split"
echo "For official Stage III checklist flow use: bash scripts/stage3.sh"
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

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

SCRIPT_PATH="${ROOT}/scripts/stage3_dummy_classifier.py"
SCRIPT_PATH="$(python_script_path_for_platform "${SCRIPT_PATH}")"

echo "Stage 3 dummy baseline: majority-class classifier"
"${PYTHON_CMD[@]}" "${SCRIPT_PATH}"

for artifact in \
  "output/stage3_dummy_metrics.json" \
  "output/stage3_dummy_metrics.txt"; do
  if [[ ! -s "${artifact}" ]]; then
    echo "Missing or empty artifact: ${artifact}" >&2
    exit 1
  fi
done

echo "Stage 3 dummy baseline completed"

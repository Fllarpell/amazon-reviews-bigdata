#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

# Local / CI smoke test: majority-class baseline (no Spark). Set STAGE3_DUMMY_ONLY=1.
if [[ "${STAGE3_DUMMY_ONLY:-0}" == "1" ]]; then
  SCRIPT_PATH="${ROOT}/scripts/legacy/stage3_dummy_classifier.py"
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
  exit 0
fi

echo "stage3_dummy.sh is deprecated. Running official Stage 3 entrypoint (scripts/stage3.sh)."
bash "${ROOT}/scripts/stage3.sh"

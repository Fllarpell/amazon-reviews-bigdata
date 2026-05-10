#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
resolve_python_cmd "${ROOT}"

"${PYTHON_CMD[@]}" "${ROOT}/db/load_into_postgres.py"

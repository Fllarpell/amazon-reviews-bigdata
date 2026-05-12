#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

echo "scripts/stage3_prep.sh is a compatibility wrapper."
echo "Executing legacy helper: scripts/legacy/stage3_prep.sh"
bash "${ROOT}/scripts/legacy/stage3_prep.sh"

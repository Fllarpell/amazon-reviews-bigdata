#!/usr/bin/env bash
# Remove generated artifacts for local runs and (optionally) Stage III HDFS paths on the cluster.
# Usage:
#   ./scripts/clean_artifacts.sh
#   ./scripts/clean_artifacts.sh --with-raw
#   ./scripts/clean_artifacts.sh --skip-hdfs    # only local files; no hdfs dfs
#   ./scripts/clean_artifacts.sh --with-raw --skip-hdfs
#   ./scripts/clean_artifacts.sh --empty-trash   # optional: expunge old .Trash after deletes
# HDFS deletes use -skipTrash so space is freed immediately (otherwise data stays under .Trash).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

with_raw=0
skip_hdfs=0
empty_trash=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-raw) with_raw=1 ;;
    --skip-hdfs) skip_hdfs=1 ;;
    --empty-trash) empty_trash=1 ;;
    *)
      echo "Usage: $0 [--with-raw] [--skip-hdfs] [--empty-trash]" >&2
      echo "  --with-raw      also remove data/raw JSONL" >&2
      echo "  --skip-hdfs     do not run hdfs dfs (local cleanup only)" >&2
      echo "  --empty-trash   after Stage III rm, run hdfs dfs -expunge (empties .Trash when eligible)" >&2
      exit 1
      ;;
  esac
  shift
done

rm -f output/hive_results.txt output/q1.csv output/q2.csv output/q3.csv output/q4.csv
rm -f output/q1.jpg output/q2.jpg output/q3.jpg output/q4.jpg
rm -f output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv
rm -f output/data_profile_before.csv output/data_profile_after.csv output/data_quality_report.txt
rm -f data/staging/reviews.csv data/staging/metadata.csv
rm -f data/processed/ml_dataset.csv data/train.json data/test.json

# Stage III (local mirrors, interpretability, Hive log from beeline step)
rm -f data/feature_manifest.json
rm -f output/stage3_hive_results.txt
rm -rf output/interpretability

rm -rf .pytest_cache
rm -rf .mypy_cache
rm -rf __pycache__
rm -rf config/__pycache__
rm -rf db/__pycache__
rm -rf etl/__pycache__
rm -rf lib/__pycache__
rm -rf scripts/__pycache__

[[ "${with_raw}" -eq 1 ]] && rm -f data/raw/Appliances.jsonl data/raw/meta_Appliances.jsonl

# Stage III on HDFS (same path layout as scripts/stage3.sh; relative to your HDFS user home)
if [[ "${skip_hdfs}" -eq 0 ]] && command -v hdfs >/dev/null 2>&1; then
  HDFS_DATA_BASE="${HDFS_DATA_BASE:-project/data}"
  HDFS_OUTPUT_BASE="${HDFS_OUTPUT_BASE:-project/output}"
  HDFS_MODEL_BASE="${HDFS_MODEL_BASE:-project/models}"
  echo "Removing Stage III HDFS paths (skipTrash): data base=${HDFS_DATA_BASE}, output=${HDFS_OUTPUT_BASE}, models=${HDFS_MODEL_BASE}" >&2
  set +e
  hdfs dfs -rm -r -f -skipTrash \
    "${HDFS_DATA_BASE}/train" \
    "${HDFS_DATA_BASE}/test" \
    "${HDFS_OUTPUT_BASE}/model1_predictions" \
    "${HDFS_OUTPUT_BASE}/model2_predictions" \
    "${HDFS_OUTPUT_BASE}/evaluation" \
    "${HDFS_MODEL_BASE}/model1" \
    "${HDFS_MODEL_BASE}/model2"
  set -e
  echo "HDFS Stage III cleanup finished (rm -f ignores missing paths)." >&2
  if [[ "${empty_trash}" -eq 1 ]]; then
    echo "Running hdfs dfs -expunge to purge trash recovery interval (see fs.trash.interval)..." >&2
    set +e
    hdfs dfs -expunge
    set -e
  fi
elif [[ "${skip_hdfs}" -eq 1 ]]; then
  echo "Skipping HDFS cleanup (--skip-hdfs)." >&2
else
  echo "hdfs CLI not found; skipped HDFS cleanup. Set PATH or use --skip-hdfs if intentional." >&2
fi

echo "Cleanup complete."
[[ "${with_raw}" -eq 1 ]] && echo "Raw JSONL files removed."

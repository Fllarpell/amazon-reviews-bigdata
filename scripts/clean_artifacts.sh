#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

with_raw=0
[[ "${1:-}" == "--with-raw" ]] && with_raw=1

rm -f output/hive_results.txt output/q1.csv output/q2.csv output/q3.csv output/q4.csv
rm -f output/q1.jpg output/q2.jpg output/q3.jpg output/q4.jpg
rm -f output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv
rm -f output/data_profile_before.csv output/data_profile_after.csv output/data_quality_report.txt
rm -f data/staging/reviews.csv data/staging/metadata.csv
rm -f data/processed/ml_dataset.csv data/train.json data/test.json

rm -rf .pytest_cache
rm -rf .mypy_cache
rm -rf __pycache__
rm -rf config/__pycache__
rm -rf db/__pycache__
rm -rf etl/__pycache__
rm -rf lib/__pycache__
rm -rf scripts/__pycache__

[[ "${with_raw}" -eq 1 ]] && rm -f data/raw/Appliances.jsonl data/raw/meta_Appliances.jsonl

echo "Cleanup complete."
[[ "${with_raw}" -eq 1 ]] && echo "Raw JSONL files removed."

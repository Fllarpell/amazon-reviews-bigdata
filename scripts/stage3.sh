#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

HIVE_DB_NAME="${HIVE_DB_NAME:-team34_projectdb}"
HIVE_DB_LOCATION="${HIVE_DB_LOCATION:-/user/team34/project/hive/warehouse/team34_projectdb}"
HIVE_ML_FEATURES_TABLE="${HIVE_ML_FEATURES_TABLE:-ml_features}"
HIVE_ML_FEATURES_PATH="${HIVE_ML_FEATURES_PATH:-${HIVE_DB_LOCATION}/${HIVE_ML_FEATURES_TABLE}}"
HIVE_USER="${HIVE_USER:-${USER:-team34}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"

STAGE3_HDFS_TRAIN_PATH="${STAGE3_HDFS_TRAIN_PATH:-project/data/train}"
STAGE3_HDFS_TEST_PATH="${STAGE3_HDFS_TEST_PATH:-project/data/test}"
STAGE3_SPLIT_SEED="${STAGE3_SPLIT_SEED:-34}"
STAGE3_TRAIN_RATIO="${STAGE3_TRAIN_RATIO:-0.8}"

SUMMARY_OUT="${ROOT}/output/stage3_prep_summary.txt"
CLASS_BALANCE_OUT="${ROOT}/output/stage3_class_balance.csv"

TRAIN_JSON_LOCAL="${ROOT}/data/train.json"
TEST_JSON_LOCAL="${ROOT}/data/test.json"

mkdir -p "${ROOT}/output" "${ROOT}/data"

if [[ -z "${HIVE_PASSWORD:-}" ]]; then
  echo "HIVE_PASSWORD is required for Stage 3 Hive preparation." >&2
  exit 1
fi

if ! command -v beeline >/dev/null 2>&1; then
  echo "beeline is required for Stage 3 Hive preparation." >&2
  exit 1
fi

if [[ -n "${SPARK_SUBMIT_CMD:-}" ]]; then
  SPARK_SUBMIT_BIN="${SPARK_SUBMIT_CMD}"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT_BIN="spark-submit"
elif [[ -x "/usr/bin/spark-submit" ]]; then
  SPARK_SUBMIT_BIN="/usr/bin/spark-submit"
else
  echo "spark-submit is required for Stage 3." >&2
  exit 1
fi

if ! command -v hdfs >/dev/null 2>&1; then
  echo "hdfs command is required for Stage 3 artifact export." >&2
  exit 1
fi

echo "Stage 3: build ML feature table in Hive"
beeline -u "${HIVE_JDBC_URL}" -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" \
  --hiveconf "hive_db_name=${HIVE_DB_NAME}" \
  --hiveconf "hive_db_location=${HIVE_DB_LOCATION}" \
  --hiveconf "hive_ml_features_table=${HIVE_ML_FEATURES_TABLE}" \
  --hiveconf "hive_ml_features_path=${HIVE_ML_FEATURES_PATH}" \
  -f "${ROOT}/sql/stage3_ml_features.hql" > "${ROOT}/output/stage3_hive_results.txt"

echo "Stage 3: Spark split on YARN"
SPARK_SCRIPT_PATH="${ROOT}/scripts/stage3_spark_prep.py"
SPARK_SCRIPT_PATH="$(python_script_path_for_platform "${SPARK_SCRIPT_PATH}")"

"${SPARK_SUBMIT_BIN}" --master yarn "${SPARK_SCRIPT_PATH}" \
  --hive-db-name "${HIVE_DB_NAME}" \
  --hive-features-table "${HIVE_ML_FEATURES_TABLE}" \
  --train-ratio "${STAGE3_TRAIN_RATIO}" \
  --split-seed "${STAGE3_SPLIT_SEED}" \
  --hdfs-train-path "${STAGE3_HDFS_TRAIN_PATH}" \
  --hdfs-test-path "${STAGE3_HDFS_TEST_PATH}" \
  --summary-out "${SUMMARY_OUT}" \
  --class-balance-out "${CLASS_BALANCE_OUT}"

echo "Stage 3: export HDFS train/test JSON to repository"
hdfs dfs -cat "${STAGE3_HDFS_TRAIN_PATH}"/*.json > "${TRAIN_JSON_LOCAL}"
hdfs dfs -cat "${STAGE3_HDFS_TEST_PATH}"/*.json > "${TEST_JSON_LOCAL}"

for artifact in \
  "${ROOT}/output/stage3_hive_results.txt" \
  "${SUMMARY_OUT}" \
  "${CLASS_BALANCE_OUT}" \
  "${TRAIN_JSON_LOCAL}" \
  "${TEST_JSON_LOCAL}"; do
  if [[ ! -s "${artifact}" ]]; then
    echo "Missing or empty artifact: ${artifact}" >&2
    exit 1
  fi
done

echo "Stage 3 completed"

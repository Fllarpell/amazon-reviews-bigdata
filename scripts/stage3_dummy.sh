#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

# Local / CI smoke test: majority-class baseline (no Spark). Set STAGE3_DUMMY_ONLY=1.
if [[ "${STAGE3_DUMMY_ONLY:-0}" == "1" ]]; then
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
  exit 0
fi

TEAM="${TEAM:-team34}"
HIVE_DB_NAME="${HIVE_DB_NAME:-${HIVE_DB:-team34_projectdb}}"
HIVE_DB_LOCATION="${HIVE_DB_LOCATION:-/user/team34/project/hive/warehouse/team34_projectdb}"
HIVE_ML_FEATURES_TABLE="${HIVE_ML_FEATURES_TABLE:-ml_features}"
HIVE_ML_FEATURES_PATH="${HIVE_ML_FEATURES_PATH:-${HIVE_DB_LOCATION}/${HIVE_ML_FEATURES_TABLE}}"
FEATURE_TABLE="${FEATURE_TABLE:-${HIVE_ML_FEATURES_TABLE}}"
LABEL_COL="${LABEL_COL:-label}"
HIVE_METASTORE_URI="${HIVE_METASTORE_URI:-thrift://hadoop-02.uni.innopolis.ru:9883}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-project/hive/warehouse}"
HDFS_DATA_BASE="${HDFS_DATA_BASE:-project/data}"
HDFS_OUTPUT_BASE="${HDFS_OUTPUT_BASE:-project/output}"
HDFS_MODEL_BASE="${HDFS_MODEL_BASE:-project/models}"
RUN_HIVE_FEATURES="${RUN_HIVE_FEATURES:-1}"

HIVE_USER="${HIVE_USER:-${USER:-team34}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"

if [[ -n "${SPARK_SUBMIT_CMD:-}" ]]; then
  SPARK_SUBMIT_BIN="${SPARK_SUBMIT_CMD}"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT_BIN="spark-submit"
elif [[ -x "/usr/bin/spark-submit" ]]; then
  SPARK_SUBMIT_BIN="/usr/bin/spark-submit"
else
  echo "spark-submit is required for Stage 3 ML on YARN (or set SPARK_SUBMIT_CMD)." >&2
  exit 1
fi

mkdir -p "${ROOT}/data" "${ROOT}/output" "${ROOT}/models"

if [[ "${RUN_HIVE_FEATURES}" == "1" || "${RUN_HIVE_FEATURES}" == "true" || "${RUN_HIVE_FEATURES}" == "yes" ]]; then
  echo "[Stage3] Step 0/3: create Hive ML feature table"
  mkdir -p "${ROOT}/output"
  if command -v beeline >/dev/null 2>&1 && [[ -n "${HIVE_PASSWORD:-}" ]]; then
    beeline -u "${HIVE_JDBC_URL}" -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" \
      --hiveconf "hive_db_name=${HIVE_DB_NAME}" \
      --hiveconf "hive_db_location=${HIVE_DB_LOCATION}" \
      --hiveconf "hive_ml_features_table=${HIVE_ML_FEATURES_TABLE}" \
      --hiveconf "hive_ml_features_path=${HIVE_ML_FEATURES_PATH}" \
      -f "${ROOT}/sql/stage3_ml_features.hql" > "${ROOT}/output/stage3_dummy_hive_results.txt"
  elif command -v hive >/dev/null 2>&1; then
    hive \
      --hiveconf "hive_db_name=${HIVE_DB_NAME}" \
      --hiveconf "hive_db_location=${HIVE_DB_LOCATION}" \
      --hiveconf "hive_ml_features_table=${HIVE_ML_FEATURES_TABLE}" \
      --hiveconf "hive_ml_features_path=${HIVE_ML_FEATURES_PATH}" \
      -f "${ROOT}/sql/stage3_ml_features.hql"
  else
    echo "Neither beeline (with HIVE_PASSWORD) nor hive CLI is available for feature DDL." >&2
    exit 1
  fi
fi

echo "[Stage3] Step 1/3: build train/test artifacts from Hive feature layer"
"${SPARK_SUBMIT_BIN}" --master yarn \
  "${ROOT}/scripts/stage3_prepare_split.py" \
  --team "${TEAM}" \
  --database "${HIVE_DB_NAME}" \
  --feature-table "${FEATURE_TABLE}" \
  --label-col "${LABEL_COL}" \
  --hive-metastore-uri "${HIVE_METASTORE_URI}" \
  --warehouse-dir "${WAREHOUSE_DIR}" \
  --hdfs-train-dir "${HDFS_DATA_BASE}/train" \
  --hdfs-test-dir "${HDFS_DATA_BASE}/test" \
  --local-train-json "${ROOT}/data/train.json" \
  --local-test-json "${ROOT}/data/test.json"

echo "[Stage3] Step 2/3: train/tune Spark ML models on YARN"
"${SPARK_SUBMIT_BIN}" --master yarn \
  "${ROOT}/scripts/stage3_ml_train.py" \
  --team "${TEAM}" \
  --train-path "${HDFS_DATA_BASE}/train" \
  --test-path "${HDFS_DATA_BASE}/test" \
  --hdfs-output-base "${HDFS_OUTPUT_BASE}" \
  --hdfs-model-base "${HDFS_MODEL_BASE}" \
  --local-output-dir "${ROOT}/output" \
  --hive-metastore-uri "${HIVE_METASTORE_URI}" \
  --warehouse-dir "${WAREHOUSE_DIR}"

echo "[Stage3] Step 3/3: done"
echo "[Stage3] Check:"
echo " - local: ${ROOT}/data/train.json, ${ROOT}/data/test.json"
echo " - local: ${ROOT}/output/baseline_random_forest_predictions.csv"
echo " - local: ${ROOT}/output/model*_predictions.csv, ${ROOT}/output/evaluation.csv"
echo " - hdfs : ${HDFS_DATA_BASE}/train, ${HDFS_DATA_BASE}/test"
echo " - hdfs : ${HDFS_OUTPUT_BASE}, ${HDFS_MODEL_BASE}"

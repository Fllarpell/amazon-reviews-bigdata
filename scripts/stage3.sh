#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

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
STAGE3_STORE_TOP_K="${STAGE3_STORE_TOP_K:-200}"
HIVE_USER="${HIVE_USER:-${USER:-team34}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"

mkdir -p "${ROOT}/data" "${ROOT}/output" "${ROOT}/models"

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

if [[ "${RUN_HIVE_FEATURES}" == "1" || "${RUN_HIVE_FEATURES}" == "true" || "${RUN_HIVE_FEATURES}" == "yes" ]]; then
  echo "[Stage3] Step 0/3: create Hive ML feature table"
  if [[ -z "${HIVE_PASSWORD:-}" ]]; then
    echo "HIVE_PASSWORD is required for Stage 3 Hive preparation." >&2
    exit 1
  fi
  if ! command -v beeline >/dev/null 2>&1; then
    echo "beeline is required for Stage 3 Hive preparation." >&2
    exit 1
  fi
  beeline -u "${HIVE_JDBC_URL}" -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" \
    --hiveconf "hive_db_name=${HIVE_DB_NAME}" \
    --hiveconf "hive_db_location=${HIVE_DB_LOCATION}" \
    --hiveconf "hive_ml_features_table=${HIVE_ML_FEATURES_TABLE}" \
    --hiveconf "hive_ml_features_path=${HIVE_ML_FEATURES_PATH}" \
    -f "${ROOT}/sql/stage3_ml_features.hql" > "${ROOT}/output/stage3_hive_results.txt"
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
  --store-top-k "${STAGE3_STORE_TOP_K}" \
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

for artifact in \
  "${ROOT}/data/train.json" \
  "${ROOT}/data/test.json" \
  "${ROOT}/output/model1_predictions.csv" \
  "${ROOT}/output/model2_predictions.csv" \
  "${ROOT}/output/evaluation.csv"; do
  if [[ ! -s "${artifact}" ]]; then
    echo "Missing or empty artifact: ${artifact}" >&2
    exit 1
  fi
done

echo "[Stage3] Step 3/3: done"
echo "[Stage3] Check:"
echo " - local: ${ROOT}/data/train.json, ${ROOT}/data/test.json"
echo " - local: ${ROOT}/output/model1_predictions.csv, ${ROOT}/output/model2_predictions.csv"
echo " - local: ${ROOT}/output/evaluation.csv"
echo " - features: verified_purchase + main_category + store(top-${STAGE3_STORE_TOP_K}) + numeric"
echo " - hdfs : ${HDFS_DATA_BASE}/train, ${HDFS_DATA_BASE}/test"
echo " - hdfs : ${HDFS_OUTPUT_BASE}/model1_predictions, ${HDFS_OUTPUT_BASE}/model2_predictions"
echo " - hdfs : ${HDFS_MODEL_BASE}/model1, ${HDFS_MODEL_BASE}/model2"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TEAM="${TEAM:-team34}"
HIVE_DB="${HIVE_DB:-team34_projectdb}"
FEATURE_TABLE="${FEATURE_TABLE:-stage3_ml_features}"
LABEL_COL="${LABEL_COL:-label}"
HIVE_METASTORE_URI="${HIVE_METASTORE_URI:-thrift://hadoop-02.uni.innopolis.ru:9883}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-project/hive/warehouse}"
HDFS_DATA_BASE="${HDFS_DATA_BASE:-project/data}"
HDFS_OUTPUT_BASE="${HDFS_OUTPUT_BASE:-project/output}"
HDFS_MODEL_BASE="${HDFS_MODEL_BASE:-project/models}"
RUN_HIVE_FEATURES="${RUN_HIVE_FEATURES:-1}"

mkdir -p "${ROOT}/data" "${ROOT}/output" "${ROOT}/models"

if [[ "${RUN_HIVE_FEATURES}" == "1" || "${RUN_HIVE_FEATURES}" == "true" || "${RUN_HIVE_FEATURES}" == "yes" ]]; then
  echo "[Stage3] Step 0/3: create Hive ML feature table"
  hive -hivevar db_name="${HIVE_DB}" -f "${ROOT}/sql/stage3_ml_features.hql"
fi

echo "[Stage3] Step 1/3: build train/test artifacts from Hive feature layer"
spark-submit \
  --master yarn \
  "${ROOT}/scripts/stage3_prepare_split.py" \
  --team "${TEAM}" \
  --database "${HIVE_DB}" \
  --feature-table "${FEATURE_TABLE}" \
  --label-col "${LABEL_COL}" \
  --hive-metastore-uri "${HIVE_METASTORE_URI}" \
  --warehouse-dir "${WAREHOUSE_DIR}" \
  --hdfs-train-dir "${HDFS_DATA_BASE}/train" \
  --hdfs-test-dir "${HDFS_DATA_BASE}/test" \
  --local-train-json "${ROOT}/data/train.json" \
  --local-test-json "${ROOT}/data/test.json"

echo "[Stage3] Step 2/3: train/tune Spark ML models on YARN"
spark-submit \
  --master yarn \
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

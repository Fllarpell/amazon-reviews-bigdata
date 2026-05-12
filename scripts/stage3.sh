#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"
resolve_python_cmd "${ROOT}"

PY_FOR_SPARK="${PYSPARK_PYTHON:-${PYTHON_CMD[0]}}"
if [[ -z "${PYSPARK_PYTHON:-}" ]] && ! "${PY_FOR_SPARK}" -c "import numpy" >/dev/null 2>&1; then
  if [[ -x "/usr/bin/python3" ]] && /usr/bin/python3 -c "import numpy" >/dev/null 2>&1; then
    PY_FOR_SPARK="/usr/bin/python3"
    echo "[Stage3] Using /usr/bin/python3 for Spark (numpy missing for ${PYTHON_CMD[0]})" >&2
  fi
fi
export PYSPARK_PYTHON="${PY_FOR_SPARK}"
SPARK_SUBMIT_PY_ARGS=(
  --conf "spark.pyspark.python=${PY_FOR_SPARK}"
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PY_FOR_SPARK}"
  --conf "spark.executorEnv.PYSPARK_PYTHON=${PY_FOR_SPARK}"
)

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

stage3_export_spark_pythonpath() {
  local py4j
  export PYTHONPATH="${SPARK_HOME}/python${PYTHONPATH:+:${PYTHONPATH}}"
  shopt -s nullglob
  for py4j in "${SPARK_HOME}"/python/lib/py4j-*-src.zip; do
    export PYTHONPATH="${py4j}:${PYTHONPATH}"
    break
  done
  shopt -u nullglob
}

stage3_try_known_spark_roots() {
  local cand resolved
  local -a roots
  if [[ -n "${SPARK_HOME_CANDIDATES:-}" ]]; then
    IFS=':' read -ra roots <<< "${SPARK_HOME_CANDIDATES}"
  fi
  roots+=(
    "/usr/lib/spark3"
    "/usr/lib/spark"
    "/usr/hdp/current/spark3-client"
    "/usr/hdp/current/spark2-client"
    "/opt/spark"
  )
  for cand in "${roots[@]}"; do
    [[ -z "${cand// }" ]] && continue
    resolved="$(readlink -f "${cand}" 2>/dev/null || echo "${cand}")"
    if [[ -d "${resolved}/python/pyspark" ]]; then
      export SPARK_HOME="${resolved}"
      stage3_export_spark_pythonpath
      echo "[Stage3] SPARK_HOME from directory search: ${SPARK_HOME}"
      return 0
    fi
  done
  return 1
}

stage3_resolve_spark_home() {
  local sub sub_real candidate
  if [[ -n "${SPARK_HOME:-}" ]]; then
    if [[ -d "${SPARK_HOME}/python/pyspark" ]]; then
      export SPARK_HOME
      stage3_export_spark_pythonpath
      echo "[Stage3] Using SPARK_HOME=${SPARK_HOME}"
      return 0
    fi
    echo "[Stage3] SPARK_HOME=${SPARK_HOME} is set but ${SPARK_HOME}/python/pyspark is missing; fix SPARK_HOME." >&2
    return 1
  fi

  sub="${SPARK_SUBMIT_BIN}"
  if [[ "${sub}" != /* ]]; then
    sub="$(command -v "${sub}" 2>/dev/null || true)"
  fi
  if [[ -z "${sub}" || ! -e "${sub}" ]]; then
    echo "[Stage3] Cannot locate spark-submit to infer SPARK_HOME; set SPARK_HOME in the environment or .env." >&2
    return 1
  fi
  sub_real="$(readlink -f "${sub}" 2>/dev/null || readlink "${sub}" 2>/dev/null || echo "${sub}")"
  candidate="$(cd "$(dirname "${sub_real}")/.." && pwd)"
  if [[ -d "${candidate}/python/pyspark" ]]; then
    export SPARK_HOME="${candidate}"
    stage3_export_spark_pythonpath
    echo "[Stage3] SPARK_HOME was unset; inferred SPARK_HOME=${SPARK_HOME} from spark-submit path"
    return 0
  fi

  if stage3_try_known_spark_roots; then
    return 0
  fi

  echo "[Stage3] Could not find Spark (no python/pyspark). spark-submit is ${sub_real} (tried parent ${candidate})." >&2
  echo "[Stage3] Set SPARK_HOME in .env to the install root, or SPARK_HOME_CANDIDATES=path1:path2 with colon-separated roots." >&2
  echo "[Stage3] On HDP-style nodes try: ls /usr/hdp/current" >&2
  return 1
}

stage3_resolve_spark_home || exit 1

SPARK_ANTLR_CONF_ARGS=()
stage3_spark_antlr_classpath() {
  local jar="${STAGE3_ANTLR_RUNTIME_JAR:-}"
  if [[ -z "${jar}" ]]; then
    shopt -s nullglob
    for candidate in "${SPARK_HOME}/jars"/antlr4-runtime-*.jar; do
      jar="${candidate}"
      break
    done
    shopt -u nullglob
  fi
  if [[ -n "${jar}" && -f "${jar}" ]]; then
    SPARK_ANTLR_CONF_ARGS=(
      --conf "spark.driver.extraClassPath=${jar}"
      --conf "spark.executor.extraClassPath=${jar}"
      --conf "spark.driver.userClassPathFirst=true"
      --conf "spark.executor.userClassPathFirst=true"
    )
    echo "[Stage3] spark.{driver,executor}: extraClassPath ANTLR ${jar} + userClassPathFirst" >&2
  else
    echo "[Stage3] No antlr4-runtime jar under ${SPARK_HOME}/jars; set STAGE3_ANTLR_RUNTIME_JAR if SQL fails" >&2
  fi
}
stage3_spark_antlr_classpath

if ! "${PY_FOR_SPARK}" -c "import numpy" >/dev/null 2>&1; then
  echo "[Stage3] NumPy is required for PySpark ML on the driver (${PY_FOR_SPARK})." >&2
  echo "[Stage3] Install: ${PY_FOR_SPARK} -m pip install --user -r requirements.txt" >&2
  echo "[Stage3] Or set PYSPARK_PYTHON to the interpreter where you ran pip (e.g. /usr/bin/python3)." >&2
  exit 1
fi

if ! command -v hdfs >/dev/null 2>&1; then
  echo "hdfs is required for Stage 3 HDFS paths (mkdir + spark I/O)." >&2
  exit 1
fi

echo "[Stage3] Ensure HDFS directories exist: ${HDFS_DATA_BASE}, ${HDFS_OUTPUT_BASE}, ${HDFS_MODEL_BASE}"
hdfs dfs -mkdir -p "${HDFS_DATA_BASE}" "${HDFS_OUTPUT_BASE}" "${HDFS_MODEL_BASE}"

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
  "${SPARK_SUBMIT_PY_ARGS[@]}" \
  "${SPARK_ANTLR_CONF_ARGS[@]}" \
  "${ROOT}/scripts/stage3_prepare_split.py" \
  --team "${TEAM}" \
  --database "${HIVE_DB_NAME}" \
  --feature-table "${FEATURE_TABLE}" \
  --features-hdfs-path "${HIVE_ML_FEATURES_PATH}" \
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
  "${SPARK_SUBMIT_PY_ARGS[@]}" \
  "${SPARK_ANTLR_CONF_ARGS[@]}" \
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

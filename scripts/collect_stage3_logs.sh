#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"

LOG_DIR="${ROOT}/output/logs_stage3"
mkdir -p "${LOG_DIR}"

overall_exit=0

echo "=== stage3_run ===" | tee "${LOG_DIR}/stage3_run.txt"
bash "${ROOT}/scripts/stage3.sh" 2>&1 | tee -a "${LOG_DIR}/stage3_run.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage3_run.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage3_artifacts_local ===" | tee "${LOG_DIR}/stage3_artifacts_local.txt"
ls -lh \
  "${ROOT}/data/train.json" \
  "${ROOT}/data/test.json" \
  "${ROOT}/output/model1_predictions.csv" \
  "${ROOT}/output/model2_predictions.csv" \
  "${ROOT}/output/evaluation.csv" 2>&1 | tee -a "${LOG_DIR}/stage3_artifacts_local.txt"
wc -l \
  "${ROOT}/output/model1_predictions.csv" \
  "${ROOT}/output/model2_predictions.csv" \
  "${ROOT}/output/evaluation.csv" 2>&1 | tee -a "${LOG_DIR}/stage3_artifacts_local.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage3_artifacts_local.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage3_hdfs_artifacts ===" | tee "${LOG_DIR}/stage3_hdfs_artifacts.txt"
hdfs dfs -ls -R "${HDFS_DATA_BASE:-project/data}" 2>&1 | tee -a "${LOG_DIR}/stage3_hdfs_artifacts.txt"
hdfs dfs -ls -R "${HDFS_MODEL_BASE:-project/models}" 2>&1 | tee -a "${LOG_DIR}/stage3_hdfs_artifacts.txt"
hdfs dfs -ls -R "${HDFS_OUTPUT_BASE:-project/output}" 2>&1 | tee -a "${LOG_DIR}/stage3_hdfs_artifacts.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage3_hdfs_artifacts.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage3_hive_tables ===" | tee "${LOG_DIR}/stage3_hive_tables.txt"
beeline -u "${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}" -n "${HIVE_USER:-team34}" -p "${HIVE_PASSWORD:-}" \
  -e "USE ${HIVE_DB_NAME:-team34_projectdb}; SHOW TABLES LIKE '${HIVE_ML_FEATURES_TABLE:-ml_features}';" 2>&1 | tee -a "${LOG_DIR}/stage3_hive_tables.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage3_hive_tables.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage3_evaluation_preview ===" | tee "${LOG_DIR}/stage3_evaluation_preview.txt"
eval_csv="${ROOT}/output/evaluation.csv"
if [[ -f "${eval_csv}" ]]; then
  awk 'NR<=20 {print}' "${eval_csv}" 2>&1 | tee -a "${LOG_DIR}/stage3_evaluation_preview.txt"
  rc=${PIPESTATUS[0]}
else
  echo "skip: ${eval_csv} not found (ML step may not have completed)." | tee -a "${LOG_DIR}/stage3_evaluation_preview.txt"
  rc=0
fi
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage3_evaluation_preview.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage3_error_scan ===" | tee "${LOG_DIR}/stage3_error_scan.txt"
grep -nE 'Traceback|SyntaxError:|IndentationError:|ModuleNotFoundError:|Exception in thread|Caused by:|java\.lang\.|py4j\.|org\.apache\.spark\.SparkException' \
  "${LOG_DIR}/stage3_run.txt" 2>&1 | tee -a "${LOG_DIR}/stage3_error_scan.txt"
grep_rc=${PIPESTATUS[0]}
if [[ ${grep_rc} -ne 0 ]]; then
  echo "No driver/Python/Java exception signatures matched in stage3_run.txt" | tee -a "${LOG_DIR}/stage3_error_scan.txt"
fi
echo "grep_exit=${grep_rc}" | tee -a "${LOG_DIR}/stage3_error_scan.txt"

echo "Logs saved in ${LOG_DIR}"
echo "overall_exit=${overall_exit}"
exit "${overall_exit}"

#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

source "${ROOT}/scripts/common.sh"
load_dotenv "${ROOT}"

LOG_DIR="${ROOT}/output/logs"
mkdir -p "${LOG_DIR}"

overall_exit=0

echo "=== stage1_run ===" | tee "${LOG_DIR}/stage1_run.txt"
bash "${ROOT}/scripts/stage1.sh" 2>&1 | tee -a "${LOG_DIR}/stage1_run.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage1_run.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage1_postgres_counts ===" | tee "${LOG_DIR}/stage1_postgres_counts.txt"
PGPASSWORD="$(cat "${ROOT}/secrets/.psql.pass")" psql -h "${PGHOST:-}" -p "${PGPORT:-}" -U "${PGUSER:-}" -d "${PGDATABASE:-}" -c "SELECT COUNT(*) AS metadata_rows FROM metadata; SELECT COUNT(*) AS reviews_rows FROM reviews;" 2>&1 | tee -a "${LOG_DIR}/stage1_postgres_counts.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage1_postgres_counts.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage1_hdfs_ls ===" | tee "${LOG_DIR}/stage1_hdfs_ls.txt"
hdfs dfs -ls -R "${HDFS_WAREHOUSE_BASE:-/user/team34/project/warehouse}" 2>&1 | tee -a "${LOG_DIR}/stage1_hdfs_ls.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage1_hdfs_ls.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage2_run ===" | tee "${LOG_DIR}/stage2_run.txt"
bash "${ROOT}/scripts/stage2.sh" 2>&1 | tee -a "${LOG_DIR}/stage2_run.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage2_run.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage2_artifacts ===" | tee "${LOG_DIR}/stage2_artifacts.txt"
ls -lh "${ROOT}/output/hive_results.txt" "${ROOT}/output/q1.csv" "${ROOT}/output/q2.csv" "${ROOT}/output/q3.csv" "${ROOT}/output/q4.csv" 2>&1 | tee -a "${LOG_DIR}/stage2_artifacts.txt"
wc -l "${ROOT}/output/q1.csv" "${ROOT}/output/q2.csv" "${ROOT}/output/q3.csv" "${ROOT}/output/q4.csv" 2>&1 | tee -a "${LOG_DIR}/stage2_artifacts.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage2_artifacts.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage2_hive_tables ===" | tee "${LOG_DIR}/stage2_hive_tables.txt"
beeline -u "${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}" -n "${HIVE_USER:-team34}" -p "${HIVE_PASSWORD:-}" -e "USE ${HIVE_DB_NAME:-team34_projectdb}; SHOW TABLES;" 2>&1 | tee -a "${LOG_DIR}/stage2_hive_tables.txt"
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage2_hive_tables.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "=== stage2_error_scan ===" | tee "${LOG_DIR}/stage2_error_scan.txt"
grep -nE 'FAILED|Error|Exception' "${ROOT}/output/hive_results.txt" 2>&1 | tee -a "${LOG_DIR}/stage2_error_scan.txt"
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
  echo "No FAILED/Error/Exception" | tee -a "${LOG_DIR}/stage2_error_scan.txt"
fi
rc=${PIPESTATUS[0]}
echo "exit_code=${rc}" | tee -a "${LOG_DIR}/stage2_error_scan.txt"
if [[ ${rc} -ne 0 ]]; then overall_exit=${rc}; fi

echo "Logs saved in ${LOG_DIR}"
echo "overall_exit=${overall_exit}"
exit "${overall_exit}"

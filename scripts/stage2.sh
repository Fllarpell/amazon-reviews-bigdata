#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

STAGE2_ENGINE="${STAGE2_ENGINE:-beeline}"
HIVE_DB_NAME="${HIVE_DB_NAME:-team34_projectdb}"
HIVE_DB_LOCATION="${HIVE_DB_LOCATION:-/user/team34/project/hive/warehouse/team34_projectdb}"
HDFS_WAREHOUSE_BASE="${HDFS_WAREHOUSE_BASE:-/user/team34/project/warehouse}"
HIVE_REVIEWS_OPTIMIZED_PATH="${HIVE_REVIEWS_OPTIMIZED_PATH:-${HIVE_DB_LOCATION}/reviews_optimized}"
HIVE_METADATA_BUCKETED_PATH="${HIVE_METADATA_BUCKETED_PATH:-${HIVE_DB_LOCATION}/metadata_bucketed}"

run_stage2_beeline() {
  if ! command -v beeline >/dev/null 2>&1; then
    echo "beeline is required for STAGE2_ENGINE=beeline" >&2
    exit 1
  fi

  local hive_user="${HIVE_USER:-${USER:-team34}}"
  local hive_jdbc_url="${HIVE_JDBC_URL:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"
  if [[ -z "${HIVE_PASSWORD:-}" ]]; then
    echo "HIVE_PASSWORD is required for STAGE2_ENGINE=beeline to avoid interactive beeline prompts." >&2
    exit 1
  fi
  local -a beeline_base=(beeline -u "${hive_jdbc_url}" -n "${hive_user}" -p "${HIVE_PASSWORD}")

  local -a hiveconf_args=(
    --hiveconf "hive_db_name=${HIVE_DB_NAME}"
    --hiveconf "hive_db_location=${HIVE_DB_LOCATION}"
    --hiveconf "hdfs_warehouse_base=${HDFS_WAREHOUSE_BASE}"
    --hiveconf "hive_reviews_optimized_path=${HIVE_REVIEWS_OPTIMIZED_PATH}"
    --hiveconf "hive_metadata_bucketed_path=${HIVE_METADATA_BUCKETED_PATH}"
  )

  mkdir -p output
  echo "Stage 2: Hive DDL + EDA via Beeline"
  echo "Running stage2_hive_init.hql..."
  "${beeline_base[@]}" "${hiveconf_args[@]}" -f "${ROOT}/sql/stage2_hive_init.hql" | tee output/hive_results.txt

  run_eda_query() {
    local query_id="$1"
    local query_file="${ROOT}/sql/${query_id}.hql"
    local output_file="output/${query_id}.csv"
    local tmp_query_file
    local started_at
    local ended_at

    if [[ ! -f "${query_file}" ]]; then
      echo "Missing query file: ${query_file}" >&2
      exit 1
    fi

    tmp_query_file="$(mktemp)"
    started_at="$(date +%s)"
    {
      printf 'USE %s;\n' "${HIVE_DB_NAME}"
      cat "${query_file}"
      printf '\n;\n'
    } > "${tmp_query_file}"

    echo "Running ${query_id}.hql..."
    "${beeline_base[@]}" --silent=true --showHeader=true --outputformat=csv2 \
      -f "${tmp_query_file}" > "${output_file}"
    rm -f "${tmp_query_file}"

    ended_at="$(date +%s)"
    echo "Saved ${output_file} (elapsed: $((ended_at - started_at))s)"
  }

  for query_id in q1 q2 q3; do
    run_eda_query "${query_id}"
  done
}

run_stage2_spark() {
  local -a PYTHON_CMD=()
  if [[ -n "${PYTHON:-}" ]]; then
    PYTHON_CMD=("${PYTHON}")
  elif [[ -x "${ROOT}/.venv/bin/python" ]]; then
    PYTHON_CMD=("${ROOT}/.venv/bin/python")
  elif command -v py >/dev/null 2>&1; then
    PYTHON_CMD=(py -3)
  elif command -v py.exe >/dev/null 2>&1; then
    PYTHON_CMD=(py.exe -3)
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
  else
    echo "No Python interpreter found (tried .venv/bin/python, python3, py)." >&2
    exit 1
  fi

  local script_path="${ROOT}/scripts/stage2_spark_eda.py"
  if [[ "${PYTHON_CMD[0]}" =~ ^(py|py\.exe|python\.exe)$ ]]; then
    if command -v cygpath >/dev/null 2>&1; then
      script_path="$(cygpath -w "${script_path}")"
    elif [[ "${script_path}" == /mnt/* ]]; then
      local drive_letter
      local suffix
      drive_letter="$(echo "${script_path}" | cut -d'/' -f3 | tr '[:lower:]' '[:upper:]')"
      suffix="$(echo "${script_path}" | cut -d'/' -f4- | sed 's#/#\\\\#g')"
      script_path="${drive_letter}:\\${suffix}"
    fi
  fi

  echo "Stage 2: Hive DDL + Spark SQL EDA"
  "${PYTHON_CMD[@]}" "${script_path}" \
    --mode all \
    --hive-db-name "${HIVE_DB_NAME}" \
    --hive-db-location "${HIVE_DB_LOCATION}"
}

if [[ "${STAGE2_ENGINE}" == "beeline" ]]; then
  run_stage2_beeline
elif [[ "${STAGE2_ENGINE}" == "spark" ]]; then
  run_stage2_spark
else
  echo "Unsupported STAGE2_ENGINE=${STAGE2_ENGINE}. Use beeline or spark." >&2
  exit 1
fi

for csv_file in output/q1.csv output/q2.csv output/q3.csv; do
  if [[ ! -s "${csv_file}" ]]; then
    echo "Missing or empty ${csv_file}" >&2
    exit 1
  fi
done

if [[ ! -s output/hive_results.txt ]]; then
  echo "Missing or empty output/hive_results.txt" >&2
  exit 1
fi

echo "Stage 2 completed"

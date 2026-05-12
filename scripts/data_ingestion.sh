#!/usr/bin/env bash
# Sqoop Parquet export and upload of staging CSV to HDFS.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

bash "${ROOT}/export/sqoop_parquet_export.sh"
bash "${ROOT}/export/hdfs_upload_staging.sh"

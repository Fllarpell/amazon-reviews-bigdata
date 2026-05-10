#!/usr/bin/env bash
set -euo pipefail
ROLE="${APP_DB_USER:-pipeline_app}"
DBNAME="${APP_DB_NAME:-review_analytics}"
PW="${POSTGRES_PASSWORD:-postgres}"
psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<-EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${ROLE}') THEN
    CREATE ROLE ${ROLE} LOGIN PASSWORD '${PW}';
  END IF;
END
\$\$;
EOSQL
if ! psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DBNAME}'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 -U postgres -d postgres -c "CREATE DATABASE ${DBNAME} OWNER ${ROLE};"
fi
psql -v ON_ERROR_STOP=1 -U postgres -d "${DBNAME}" <<-EOSQL
SELECT 1;
EOSQL

#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -p reviewhdfs -f "${ROOT}/infra/hadoop/docker-compose.yml")
max="${1:-90}"
i=0
while (( i < max )); do
  mode="$("${COMPOSE[@]}" exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null || true)"
  if echo "$mode" | grep -Eqi 'off'; then
    if "${COMPOSE[@]}" exec -T namenode hdfs dfs -mkdir -p /tmp 2>/dev/null; then
      echo "HDFS namenode ready"
      exit 0
    fi
  fi
  sleep 2
  i=$((i + 1))
done
echo "timeout waiting for HDFS" >&2
exit 1

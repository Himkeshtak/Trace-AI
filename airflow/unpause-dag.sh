#!/usr/bin/env bash
set -euo pipefail

dag_id="${TRACEAI_AIRFLOW_DAG_ID:-traceai_daily_metrics}"
deadline=$((SECONDS + 120))

until airflow dags list 2>/dev/null | awk '{print $1}' | grep -qx "$dag_id"; do
    if ((SECONDS >= deadline)); then
        echo "Timed out waiting for Airflow to parse DAG ${dag_id}" >&2
        airflow dags list >&2 || true
        exit 1
    fi
    sleep 2
done

airflow dags unpause "$dag_id"

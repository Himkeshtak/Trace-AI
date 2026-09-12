from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


AGGREGATION_SQL = """
WITH execution_daily AS (
    SELECT
        DATE(started_at) AS metric_date,
        COUNT(*) AS total_executions,
        COUNT(*) FILTER (WHERE status = 'success') AS successful_executions,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed_executions,
        COALESCE(AVG(latency_ms) FILTER (WHERE latency_ms IS NOT NULL), 0)
            AS average_latency_ms
    FROM executions
    WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(started_at)
),
usage_daily AS (
    SELECT
        DATE(created_at) AS metric_date,
        SUM(input_tokens) AS input_tokens,
        SUM(output_tokens) AS output_tokens,
        SUM(estimated_cost_usd) AS estimated_cost_usd
    FROM llm_usage
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(created_at)
),
combined AS (
    SELECT
        COALESCE(e.metric_date, u.metric_date) AS metric_date,
        COALESCE(e.total_executions, 0) AS total_executions,
        COALESCE(e.successful_executions, 0) AS successful_executions,
        COALESCE(e.failed_executions, 0) AS failed_executions,
        COALESCE(e.average_latency_ms, 0) AS average_latency_ms,
        COALESCE(u.input_tokens, 0) AS input_tokens,
        COALESCE(u.output_tokens, 0) AS output_tokens,
        COALESCE(u.estimated_cost_usd, 0) AS estimated_cost_usd
    FROM execution_daily e
    FULL OUTER JOIN usage_daily u USING (metric_date)
)
INSERT INTO daily_metrics (
    metric_date,
    total_executions,
    successful_executions,
    failed_executions,
    average_latency_ms,
    input_tokens,
    output_tokens,
    estimated_cost_usd,
    calculated_at
)
SELECT
    metric_date,
    total_executions,
    successful_executions,
    failed_executions,
    ROUND(average_latency_ms, 2),
    input_tokens,
    output_tokens,
    estimated_cost_usd,
    NOW()
FROM combined
ON CONFLICT (metric_date) DO UPDATE SET
    total_executions = EXCLUDED.total_executions,
    successful_executions = EXCLUDED.successful_executions,
    failed_executions = EXCLUDED.failed_executions,
    average_latency_ms = EXCLUDED.average_latency_ms,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    estimated_cost_usd = EXCLUDED.estimated_cost_usd,
    calculated_at = EXCLUDED.calculated_at;
"""


with DAG(
    dag_id="traceai_daily_metrics",
    description="Rebuild the last 30 days of TraceAI analytical summaries.",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["traceai", "analytics"],
) as dag:
    aggregate_daily_metrics = SQLExecuteQueryOperator(
        task_id="aggregate_daily_metrics",
        conn_id="traceai_postgres",
        sql=AGGREGATION_SQL,
        autocommit=True,
    )


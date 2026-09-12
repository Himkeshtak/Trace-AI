import os
from datetime import UTC, date, datetime, timedelta

import psycopg2
import redis
from airflow import DAG
from airflow.operators.python import PythonOperator


def as_int(values: dict[str, str], field: str) -> int:
    return int(float(values.get(field, "0") or 0))


def as_float(values: dict[str, str], field: str) -> float:
    return float(values.get(field, "0") or 0)


def archive_daily_metrics() -> None:
    redis_client = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    earliest = datetime.now(UTC).date() - timedelta(days=30)
    keys = sorted(redis_client.smembers("trace:daily_keys"))
    rows = []
    for key in keys:
        metric_date = date.fromisoformat(key.rsplit(":", 1)[-1])
        if metric_date < earliest:
            continue
        values = redis_client.hgetall(key)
        completed = as_int(values, "completed_executions")
        rows.append(
            (
                metric_date,
                as_int(values, "total_executions"),
                as_int(values, "successful_executions"),
                as_int(values, "failed_executions"),
                round(as_float(values, "latency_total_ms") / completed, 2)
                if completed
                else 0,
                as_int(values, "input_tokens"),
                as_int(values, "output_tokens"),
                as_float(values, "estimated_cost_usd"),
            )
        )
    redis_client.close()
    if not rows:
        return

    statement = """
        INSERT INTO daily_metrics (
            metric_date, total_executions, successful_executions, failed_executions,
            average_latency_ms, input_tokens, output_tokens, estimated_cost_usd,
            calculated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (metric_date) DO UPDATE SET
            total_executions = EXCLUDED.total_executions,
            successful_executions = EXCLUDED.successful_executions,
            failed_executions = EXCLUDED.failed_executions,
            average_latency_ms = EXCLUDED.average_latency_ms,
            input_tokens = EXCLUDED.input_tokens,
            output_tokens = EXCLUDED.output_tokens,
            estimated_cost_usd = EXCLUDED.estimated_cost_usd,
            calculated_at = EXCLUDED.calculated_at
    """
    with psycopg2.connect(os.environ["TRACEAI_DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(statement, rows)


with DAG(
    dag_id="traceai_daily_metrics",
    description="Archive Redis hot metrics as durable PostgreSQL daily summaries.",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["traceai", "redis", "analytics"],
) as dag:
    archive_metrics = PythonOperator(
        task_id="archive_redis_daily_metrics",
        python_callable=archive_daily_metrics,
    )


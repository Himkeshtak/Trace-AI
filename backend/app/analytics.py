from decimal import Decimal
from typing import Any

from backend.app.infrastructure import AnalyticsStore


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


async def load_summary(store: AnalyticsStore) -> dict[str, int | float]:
    row = await store.fetch_one(
        """
        SELECT
            COUNT(*) AS total_executions,
            COUNT(*) FILTER (WHERE status = 'success') AS successful_executions,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed_executions,
            COALESCE(AVG(latency_ms) FILTER (WHERE latency_ms IS NOT NULL), 0)
                AS average_latency_ms,
            COALESCE((SELECT SUM(input_tokens) FROM llm_usage), 0) AS input_tokens,
            COALESCE((SELECT SUM(output_tokens) FROM llm_usage), 0) AS output_tokens,
            COALESCE((SELECT SUM(estimated_cost_usd) FROM llm_usage), 0)
                AS estimated_cost_usd,
            (SELECT COUNT(*) FROM memory_accesses) AS memory_accesses
        FROM executions
        """
    )
    total = int(row.get("total_executions", 0))
    successful = int(row.get("successful_executions", 0))
    return {
        "total_executions": total,
        "successful_executions": successful,
        "failed_executions": int(row.get("failed_executions", 0)),
        "success_rate": round(successful / total * 100, 1) if total else 0.0,
        "average_latency_ms": round(_number(row.get("average_latency_ms")), 1),
        "input_tokens": int(row.get("input_tokens", 0)),
        "output_tokens": int(row.get("output_tokens", 0)),
        "estimated_cost_usd": round(_number(row.get("estimated_cost_usd")), 6),
        "memory_accesses": int(row.get("memory_accesses", 0)),
    }


async def load_tool_metrics(store: AnalyticsStore) -> list[dict[str, Any]]:
    rows = await store.fetch_all(
        """
        SELECT
            tool_name,
            COUNT(*) AS call_count,
            ROUND(AVG(response_time_ms), 1) AS average_response_time_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms)::numeric, 1)
                AS p95_response_time_ms,
            ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / COUNT(*), 1)
                AS success_rate
        FROM tool_calls
        GROUP BY tool_name
        ORDER BY average_response_time_ms DESC
        LIMIT 10
        """
    )
    return [
        {
            **row,
            "call_count": int(row["call_count"]),
            "average_response_time_ms": _number(row["average_response_time_ms"]),
            "p95_response_time_ms": _number(row["p95_response_time_ms"]),
            "success_rate": _number(row["success_rate"]),
        }
        for row in rows
    ]


async def load_agent_metrics(store: AnalyticsStore) -> list[dict[str, Any]]:
    rows = await store.fetch_all(
        """
        SELECT
            a.agent_id,
            a.agent_name,
            a.version,
            COUNT(e.execution_id) AS execution_count,
            COUNT(e.execution_id) FILTER (WHERE e.status = 'failed') AS failure_count,
            COALESCE(ROUND(AVG(e.latency_ms) FILTER (WHERE e.latency_ms IS NOT NULL), 1), 0)
                AS average_latency_ms
        FROM agents a
        LEFT JOIN executions e ON e.agent_id = a.agent_id
        GROUP BY a.agent_id, a.agent_name, a.version
        ORDER BY failure_count DESC, execution_count DESC
        """
    )
    return [
        {
            **row,
            "agent_id": str(row["agent_id"]),
            "execution_count": int(row["execution_count"]),
            "failure_count": int(row["failure_count"]),
            "average_latency_ms": _number(row["average_latency_ms"]),
        }
        for row in rows
    ]


async def load_token_metrics(store: AnalyticsStore) -> list[dict[str, Any]]:
    rows = await store.fetch_all(
        """
        SELECT
            DATE(created_at) AS usage_date,
            SUM(input_tokens) AS input_tokens,
            SUM(output_tokens) AS output_tokens,
            SUM(estimated_cost_usd) AS estimated_cost_usd
        FROM llm_usage
        WHERE created_at >= CURRENT_DATE - INTERVAL '13 days'
        GROUP BY DATE(created_at)
        ORDER BY usage_date
        """
    )
    return [
        {
            "usage_date": row["usage_date"].isoformat(),
            "input_tokens": int(row["input_tokens"]),
            "output_tokens": int(row["output_tokens"]),
            "estimated_cost_usd": round(_number(row["estimated_cost_usd"]), 6),
        }
        for row in rows
    ]


async def load_memory_metrics(store: AnalyticsStore) -> list[dict[str, Any]]:
    rows = await store.fetch_all(
        """
        SELECT
            memory_type,
            access_type,
            COUNT(*) AS access_count,
            ROUND(AVG(retrieval_time_ms), 1) AS average_retrieval_time_ms
        FROM memory_accesses
        GROUP BY memory_type, access_type
        ORDER BY access_count DESC
        """
    )
    return [
        {
            **row,
            "access_count": int(row["access_count"]),
            "average_retrieval_time_ms": _number(row["average_retrieval_time_ms"]),
        }
        for row in rows
    ]


async def load_recent_executions(store: AnalyticsStore, limit: int) -> list[dict[str, Any]]:
    rows = await store.fetch_all(
        """
        SELECT
            e.execution_id,
            a.agent_name,
            e.task_name,
            e.status,
            e.started_at,
            e.latency_ms,
            e.error_message
        FROM executions e
        JOIN agents a ON a.agent_id = e.agent_id
        ORDER BY e.started_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [
        {
            **row,
            "execution_id": str(row["execution_id"]),
            "started_at": row["started_at"].isoformat(),
        }
        for row in rows
    ]


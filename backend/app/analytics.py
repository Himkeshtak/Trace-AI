import math
from typing import Any

from backend.app.infrastructure import HotAnalyticsStore


def _integer(values: dict[str, str], field: str) -> int:
    return int(float(values.get(field, "0") or 0))


def _number(values: dict[str, str], field: str) -> float:
    return float(values.get(field, "0") or 0)


async def load_summary(store: HotAnalyticsStore) -> dict[str, int | float]:
    values = await store.redis.hgetall("trace:summary")
    total = _integer(values, "total_executions")
    successful = _integer(values, "successful_executions")
    completed = _integer(values, "completed_executions")
    return {
        "total_executions": total,
        "successful_executions": successful,
        "failed_executions": _integer(values, "failed_executions"),
        "success_rate": round(successful / completed * 100, 1) if completed else 0.0,
        "average_latency_ms": round(_number(values, "latency_total_ms") / completed, 1)
        if completed
        else 0.0,
        "input_tokens": _integer(values, "input_tokens"),
        "output_tokens": _integer(values, "output_tokens"),
        "estimated_cost_usd": round(_number(values, "estimated_cost_usd"), 6),
        "memory_accesses": _integer(values, "memory_accesses"),
    }


async def load_tool_metrics(store: HotAnalyticsStore) -> list[dict[str, Any]]:
    names = sorted(await store.redis.smembers("trace:tools"))
    if not names:
        return []
    pipeline = store.redis.pipeline(transaction=False)
    for name in names:
        pipeline.hgetall(f"trace:tool:{name}")
        pipeline.zcard(f"trace:tool_latencies:{name}")
    raw = await pipeline.execute()
    stats = [(raw[index * 2], int(raw[index * 2 + 1])) for index in range(len(names))]

    percentile_pipeline = store.redis.pipeline(transaction=False)
    for name, (_, count) in zip(names, stats, strict=True):
        percentile_index = max(math.ceil(count * 0.95) - 1, 0)
        percentile_pipeline.zrange(
            f"trace:tool_latencies:{name}",
            percentile_index,
            percentile_index,
            withscores=True,
        )
    percentiles = await percentile_pipeline.execute()

    result = []
    for name, (values, count), percentile in zip(names, stats, percentiles, strict=True):
        calls = _integer(values, "call_count")
        successes = _integer(values, "success_count")
        result.append(
            {
                "tool_name": name,
                "call_count": calls,
                "average_response_time_ms": round(_number(values, "latency_total_ms") / calls, 1)
                if calls
                else 0.0,
                "p95_response_time_ms": round(float(percentile[0][1]), 1)
                if percentile
                else 0.0,
                "success_rate": round(successes / calls * 100, 1) if calls else 0.0,
            }
        )
    return sorted(result, key=lambda item: item["average_response_time_ms"], reverse=True)[:10]


async def load_agent_metrics(store: HotAnalyticsStore) -> list[dict[str, Any]]:
    agent_ids = sorted(await store.redis.smembers("trace:agents"))
    pipeline = store.redis.pipeline(transaction=False)
    for agent_id in agent_ids:
        pipeline.hgetall(f"trace:agent:{agent_id}")
    rows = await pipeline.execute()
    result = []
    for agent_id, values in zip(agent_ids, rows, strict=True):
        completed = _integer(values, "completed_count")
        result.append(
            {
                "agent_id": agent_id,
                "agent_name": values.get("agent_name", "Unknown agent"),
                "version": values.get("version", "unknown"),
                "execution_count": _integer(values, "execution_count"),
                "failure_count": _integer(values, "failure_count"),
                "average_latency_ms": round(_number(values, "latency_total_ms") / completed, 1)
                if completed
                else 0.0,
            }
        )
    return sorted(
        result,
        key=lambda item: (item["failure_count"], item["execution_count"]),
        reverse=True,
    )


async def load_token_metrics(store: HotAnalyticsStore) -> list[dict[str, Any]]:
    keys = sorted(await store.redis.smembers("trace:daily_keys"))[-14:]
    pipeline = store.redis.pipeline(transaction=False)
    for key in keys:
        pipeline.hgetall(key)
    rows = await pipeline.execute()
    return [
        {
            "usage_date": key.rsplit(":", 1)[-1],
            "input_tokens": _integer(values, "input_tokens"),
            "output_tokens": _integer(values, "output_tokens"),
            "estimated_cost_usd": round(_number(values, "estimated_cost_usd"), 6),
        }
        for key, values in zip(keys, rows, strict=True)
    ]


async def load_memory_metrics(store: HotAnalyticsStore) -> list[dict[str, Any]]:
    groups = sorted(await store.redis.smembers("trace:memory_groups"))
    pipeline = store.redis.pipeline(transaction=False)
    for group in groups:
        pipeline.hgetall(f"trace:memory:{group}")
    rows = await pipeline.execute()
    result = []
    for group, values in zip(groups, rows, strict=True):
        memory_type, access_type = group.rsplit(":", 1)
        count = _integer(values, "access_count")
        result.append(
            {
                "memory_type": memory_type,
                "access_type": access_type,
                "access_count": count,
                "average_retrieval_time_ms": round(
                    _number(values, "retrieval_time_total_ms") / count, 1
                )
                if count
                else 0.0,
            }
        )
    return sorted(result, key=lambda item: item["access_count"], reverse=True)


async def load_recent_executions(
    store: HotAnalyticsStore, limit: int
) -> list[dict[str, Any]]:
    execution_ids = await store.redis.zrevrange("trace:executions:recent", 0, limit - 1)
    pipeline = store.redis.pipeline(transaction=False)
    for execution_id in execution_ids:
        pipeline.hgetall(f"trace:execution:{execution_id}")
    rows = await pipeline.execute()
    return [
        {
            "execution_id": values.get("execution_id", execution_id),
            "agent_name": values.get("agent_name", "Unknown agent"),
            "task_name": values.get("task_name", "Unknown task"),
            "status": values.get("status", "running"),
            "started_at": values.get("started_at", ""),
            "latency_ms": int(values["latency_ms"]) if values.get("latency_ms") else None,
            "error_message": values.get("error_message") or None,
        }
        for execution_id, values in zip(execution_ids, rows, strict=True)
        if values
    ]


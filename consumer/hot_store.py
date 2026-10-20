import json
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

from shared.traceai_contracts import (
    ExecutionCompletedEvent,
    ExecutionStartedEvent,
    LlmUsageEvent,
    MemoryAccessEvent,
    ToolCallEvent,
    TraceEvent,
)


class RedisHotStore:
    def __init__(
        self,
        redis: Redis,
        persistence_stream: str,
        processed_ttl_seconds: int,
    ) -> None:
        self.redis = redis
        self.persistence_stream = persistence_stream
        self.processed_ttl_seconds = processed_ttl_seconds

    async def write(
        self,
        event: TraceEvent,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> bool:
        processed_key = f"trace:processed:{event.event_id}"
        agent_id = await self._execution_agent_id(event)
        for _ in range(3):
            async with self.redis.pipeline(transaction=True) as pipeline:
                try:
                    await pipeline.watch(processed_key)
                    if await pipeline.exists(processed_key):
                        await pipeline.unwatch()
                        return False
                    pipeline.multi()
                    pipeline.set(processed_key, "1", ex=self.processed_ttl_seconds)
                    stream_fields = {
                        "payload": json.dumps(event.model_dump(mode="json")),
                    }
                    if topic is not None:
                        stream_fields["source_topic"] = str(topic)
                    if partition is not None:
                        stream_fields["source_partition"] = str(partition)
                    if offset is not None:
                        stream_fields["source_offset"] = str(offset)
                    pipeline.xadd(
                        self.persistence_stream,
                        stream_fields,
                        maxlen=100_000,
                        approximate=True,
                    )
                    self._queue_projection(pipeline, event, agent_id)
                    await pipeline.execute()
                    return True
                except WatchError:
                    continue
        raise RuntimeError("Redis hot-store transaction was modified repeatedly")

    async def _execution_agent_id(self, event: TraceEvent) -> str | None:
        if isinstance(event, ExecutionStartedEvent):
            return str(event.agent_id)
        return await self.redis.hget(f"trace:execution:{event.execution_id}", "agent_id")

    def _queue_projection(self, pipeline: Any, event: TraceEvent, agent_id: str | None) -> None:
        execution_key = f"trace:execution:{event.execution_id}"
        daily_key = f"trace:daily:{event.occurred_at.date().isoformat()}"
        pipeline.sadd("trace:daily_keys", daily_key)

        if isinstance(event, ExecutionStartedEvent):
            pipeline.hset(
                execution_key,
                mapping={
                    "execution_id": str(event.execution_id),
                    "agent_id": str(event.agent_id),
                    "agent_name": event.agent_name,
                    "task_name": event.task_name,
                    "status": "running",
                    "started_at": event.occurred_at.isoformat(),
                    "latency_ms": "",
                    "error_message": "",
                },
            )
            pipeline.expire(execution_key, 604_800)
            pipeline.zadd(
                "trace:executions:recent",
                {str(event.execution_id): event.occurred_at.timestamp()},
            )
            pipeline.zremrangebyrank("trace:executions:recent", 0, -501)
            pipeline.sadd("trace:agents", str(event.agent_id))
            pipeline.hset(
                f"trace:agent:{event.agent_id}",
                mapping={"agent_name": event.agent_name, "version": event.agent_version},
            )
            pipeline.hincrby(f"trace:agent:{event.agent_id}", "execution_count", 1)
            pipeline.hincrby("trace:summary", "total_executions", 1)
            pipeline.hincrby(daily_key, "total_executions", 1)
            return

        if isinstance(event, ExecutionCompletedEvent):
            pipeline.hset(
                execution_key,
                mapping={
                    "status": event.status,
                    "ended_at": event.ended_at.isoformat(),
                    "latency_ms": event.latency_ms,
                    "error_message": event.error_message or "",
                },
            )
            status_field = (
                "successful_executions" if event.status == "success" else "failed_executions"
            )
            pipeline.hincrby("trace:summary", status_field, 1)
            pipeline.hincrby("trace:summary", "completed_executions", 1)
            pipeline.hincrbyfloat("trace:summary", "latency_total_ms", event.latency_ms)
            pipeline.hincrby(daily_key, status_field, 1)
            pipeline.hincrby(daily_key, "completed_executions", 1)
            pipeline.hincrbyfloat(daily_key, "latency_total_ms", event.latency_ms)
            if agent_id:
                agent_key = f"trace:agent:{agent_id}"
                pipeline.hincrby(agent_key, "failure_count", int(event.status == "failed"))
                pipeline.hincrby(agent_key, "completed_count", 1)
                pipeline.hincrbyfloat(agent_key, "latency_total_ms", event.latency_ms)
            return

        if isinstance(event, ToolCallEvent):
            tool_key = f"trace:tool:{event.tool_name}"
            pipeline.sadd("trace:tools", event.tool_name)
            pipeline.hincrby(tool_key, "call_count", 1)
            pipeline.hincrby(tool_key, "success_count", int(event.status == "success"))
            pipeline.hincrbyfloat(tool_key, "latency_total_ms", event.response_time_ms)
            pipeline.zadd(
                f"trace:tool_latencies:{event.tool_name}",
                {str(event.event_id): event.response_time_ms},
            )
            return

        if isinstance(event, LlmUsageEvent):
            for key in ("trace:summary", daily_key):
                pipeline.hincrby(key, "input_tokens", event.input_tokens)
                pipeline.hincrby(key, "output_tokens", event.output_tokens)
                pipeline.hincrbyfloat(key, "estimated_cost_usd", float(event.estimated_cost_usd))
            return

        if isinstance(event, MemoryAccessEvent):
            group = f"{event.memory_type}:{event.access_type}"
            memory_key = f"trace:memory:{group}"
            pipeline.sadd("trace:memory_groups", group)
            pipeline.hincrby(memory_key, "access_count", 1)
            pipeline.hincrbyfloat(
                memory_key, "retrieval_time_total_ms", event.retrieval_time_ms
            )
            pipeline.hincrby("trace:summary", "memory_accesses", 1)
            pipeline.hincrby(daily_key, "memory_accesses", 1)


from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg import AsyncConnection
    from psycopg.types.json import Jsonb
else:
    try:
        from psycopg import AsyncConnection
        from psycopg.types.json import Jsonb
    except ImportError:  # pragma: no cover
        class AsyncConnection:  # pragma: no cover
            def __class_getitem__(cls, item: Any) -> Any:
                return Any

        class Jsonb:  # pragma: no cover
            def __init__(self, obj: Any) -> None:
                self.obj = obj

from shared.traceai_contracts import (
    ExecutionCompletedEvent,
    ExecutionStartedEvent,
    LlmUsageEvent,
    MemoryAccessEvent,
    ToolCallEvent,
    TraceEvent,
)


async def persist_event(
    connection: AsyncConnection[Any],
    event: TraceEvent,
    source_topic: str | None = None,
    source_partition: int | None = None,
    source_offset: int | None = None,
) -> bool:
    payload = event.model_dump(mode="json")
    cursor = await connection.execute(
        """
        INSERT INTO processed_events(
            event_id, event_type, execution_id, payload,
            source_topic, source_partition, source_offset
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """,
        (
            event.event_id,
            event.event_type,
            event.execution_id,
            Jsonb(payload),
            source_topic,
            source_partition,
            source_offset,
        ),
    )
    if await cursor.fetchone() is None:
        return False

    if isinstance(event, ExecutionStartedEvent):
        await _persist_execution_started(connection, event)
    elif isinstance(event, ToolCallEvent):
        await _persist_tool_call(connection, event)
    elif isinstance(event, LlmUsageEvent):
        await _persist_llm_usage(connection, event)
    elif isinstance(event, MemoryAccessEvent):
        await _persist_memory_access(connection, event)
    elif isinstance(event, ExecutionCompletedEvent):
        await _persist_execution_completed(connection, event)
    return True


async def _persist_execution_started(
    connection: AsyncConnection[Any], event: ExecutionStartedEvent
) -> None:
    await connection.execute(
        """
        INSERT INTO agents(agent_id, agent_name, version, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (agent_id) DO UPDATE
        SET agent_name = EXCLUDED.agent_name, version = EXCLUDED.version
        """,
        (event.agent_id, event.agent_name, event.agent_version, event.occurred_at),
    )
    await connection.execute(
        """
        INSERT INTO executions(execution_id, agent_id, task_name, started_at, status)
        VALUES (%s, %s, %s, %s, 'running')
        ON CONFLICT (execution_id) DO NOTHING
        """,
        (event.execution_id, event.agent_id, event.task_name, event.occurred_at),
    )


async def _persist_tool_call(connection: AsyncConnection[Any], event: ToolCallEvent) -> None:
    await connection.execute(
        """
        INSERT INTO tool_calls(
            tool_call_id, execution_id, tool_name, started_at, ended_at,
            response_time_ms, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tool_call_id) DO NOTHING
        """,
        (
            event.tool_call_id,
            event.execution_id,
            event.tool_name,
            event.started_at,
            event.ended_at,
            event.response_time_ms,
            event.status,
        ),
    )


async def _persist_llm_usage(connection: AsyncConnection[Any], event: LlmUsageEvent) -> None:
    await connection.execute(
        """
        INSERT INTO llm_usage(
            usage_id, execution_id, model_name, input_tokens, output_tokens,
            estimated_cost_usd, latency_ms, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (usage_id) DO NOTHING
        """,
        (
            event.usage_id,
            event.execution_id,
            event.model_name,
            event.input_tokens,
            event.output_tokens,
            event.estimated_cost_usd,
            event.latency_ms,
            event.occurred_at,
        ),
    )


async def _persist_memory_access(
    connection: AsyncConnection[Any], event: MemoryAccessEvent
) -> None:
    await connection.execute(
        """
        INSERT INTO memory_accesses(
            access_id, execution_id, memory_type, access_type,
            retrieval_time_ms, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (access_id) DO NOTHING
        """,
        (
            event.access_id,
            event.execution_id,
            event.memory_type,
            event.access_type,
            event.retrieval_time_ms,
            event.occurred_at,
        ),
    )


async def _persist_execution_completed(
    connection: AsyncConnection[Any], event: ExecutionCompletedEvent
) -> None:
    await connection.execute(
        """
        UPDATE executions
        SET ended_at = %s, status = %s, latency_ms = %s, error_message = %s
        WHERE execution_id = %s
        """,
        (
            event.ended_at,
            event.status,
            event.latency_ms,
            event.error_message,
            event.execution_id,
        ),
    )


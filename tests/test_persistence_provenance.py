import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from consumer.hot_store import RedisHotStore
from persistence.handlers import persist_event
from persistence.worker import _parse_int_coordinate, store_messages
from shared.traceai_contracts import ExecutionStartedEvent


def test_parse_int_coordinate_valid() -> None:
    assert _parse_int_coordinate({"source_partition": "3"}, "source_partition") == 3
    assert _parse_int_coordinate({"source_offset": "0"}, "source_offset") == 0
    assert _parse_int_coordinate({}, "source_partition") is None
    assert _parse_int_coordinate({"source_partition": None}, "source_partition") is None


def test_parse_int_coordinate_invalid() -> None:
    with pytest.raises(ValueError, match="invalid literal for int()"):
        _parse_int_coordinate({"source_partition": "abc"}, "source_partition")

    with pytest.raises(ValueError, match="source_offset must be non-negative"):
        _parse_int_coordinate({"source_offset": "-5"}, "source_offset")


@pytest.mark.anyio
async def test_hot_store_write_serializes_provenance_fields() -> None:
    mock_redis = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.exists = AsyncMock(return_value=False)
    mock_pipeline.watch = AsyncMock()
    mock_pipeline.unwatch = AsyncMock()
    mock_pipeline.execute = AsyncMock()
    # pipeline context manager support
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipeline
    mock_redis.pipeline.return_value.__aexit__.return_value = False

    event = ExecutionStartedEvent(
        event_id=uuid4(),
        event_type="execution_started",
        execution_id=uuid4(),
        occurred_at="2026-10-20T12:00:00Z",
        agent_id=uuid4(),
        agent_name="Research Agent",
        agent_version="1.0.0",
        task_name="Testing Provenance",
    )

    hot_store = RedisHotStore(
        redis=mock_redis,
        persistence_stream="trace:persistence",
        processed_ttl_seconds=604800,
    )

    written = await hot_store.write(
        event,
        topic="trace-events",
        partition=2,
        offset=1042,
    )

    assert written is True
    mock_pipeline.xadd.assert_called_once()
    xadd_args = mock_pipeline.xadd.call_args
    stream_name = xadd_args[0][0]
    stream_fields = xadd_args[0][1]

    assert stream_name == "trace:persistence"
    assert "payload" in stream_fields
    assert stream_fields["source_topic"] == "trace-events"
    assert stream_fields["source_partition"] == "2"
    assert stream_fields["source_offset"] == "1042"


@pytest.mark.anyio
async def test_store_messages_parses_provenance_and_commits_before_xack() -> None:
    mock_redis = AsyncMock()
    mock_connection = AsyncMock()
    mock_transaction = AsyncMock()
    mock_connection.transaction = MagicMock(return_value=mock_transaction)
    mock_transaction.__aenter__.return_value = mock_transaction
    mock_transaction.__aexit__.return_value = False

    event_id = uuid4()
    execution_id = uuid4()
    agent_id = uuid4()
    event_dict = {
        "event_id": str(event_id),
        "event_type": "execution_started",
        "execution_id": str(execution_id),
        "occurred_at": "2026-10-20T12:00:00Z",
        "agent_id": str(agent_id),
        "agent_name": "Test Agent",
        "agent_version": "1.0",
        "task_name": "Provenance test",
    }

    mock_cursor = AsyncMock()
    mock_cursor.fetchone.return_value = (str(event_id),)
    mock_connection.execute.return_value = mock_cursor

    messages = [
        (
            "1729420000000-0",
            {
                "payload": json.dumps(event_dict),
                "source_topic": "trace-events",
                "source_partition": "1",
                "source_offset": "500",
            },
        )
    ]

    await store_messages(
        redis=mock_redis,
        connection=mock_connection,
        stream="trace:persistence",
        group="postgres-writers",
        messages=messages,
    )

    # Verify executed SQL query parameters included provenance
    assert mock_connection.execute.call_count >= 1
    first_call_args = mock_connection.execute.call_args_list[0]
    sql = first_call_args[0][0]
    params = first_call_args[0][1]

    assert "INSERT INTO processed_events" in sql
    assert params[0] == event_id
    assert params[1] == "execution_started"
    assert params[2] == execution_id
    assert params[4] == "trace-events"
    assert params[5] == 1
    assert params[6] == 500

    # Verify xack was called after transaction
    mock_redis.xack.assert_awaited_once_with("trace:persistence", "postgres-writers", "1729420000000-0")


@pytest.mark.anyio
async def test_store_messages_legacy_payload_persists_null_coordinates() -> None:
    mock_redis = AsyncMock()
    mock_connection = AsyncMock()
    mock_transaction = AsyncMock()
    mock_connection.transaction = MagicMock(return_value=mock_transaction)
    mock_transaction.__aenter__.return_value = mock_transaction
    mock_transaction.__aexit__.return_value = False

    event_id = uuid4()
    event_dict = {
        "event_id": str(event_id),
        "event_type": "execution_started",
        "execution_id": str(uuid4()),
        "occurred_at": "2026-10-20T12:00:00Z",
        "agent_id": str(uuid4()),
        "agent_name": "Legacy Agent",
        "agent_version": "1.0",
        "task_name": "Legacy test",
    }

    mock_cursor = AsyncMock()
    mock_cursor.fetchone.return_value = (str(event_id),)
    mock_connection.execute.return_value = mock_cursor

    # Legacy message with only "payload"
    messages = [
        (
            "1729420000000-1",
            {
                "payload": json.dumps(event_dict),
            },
        )
    ]

    await store_messages(
        redis=mock_redis,
        connection=mock_connection,
        stream="trace:persistence",
        group="postgres-writers",
        messages=messages,
    )

    first_call_params = mock_connection.execute.call_args_list[0][0][1]
    assert first_call_params[4] is None  # source_topic
    assert first_call_params[5] is None  # source_partition
    assert first_call_params[6] is None  # source_offset

    mock_redis.xack.assert_awaited_once_with("trace:persistence", "postgres-writers", "1729420000000-1")


@pytest.mark.anyio
async def test_store_messages_invalid_provenance_fails_and_skips_xack() -> None:
    mock_redis = AsyncMock()
    mock_connection = AsyncMock()

    event_dict = {
        "event_id": str(uuid4()),
        "event_type": "execution_started",
        "execution_id": str(uuid4()),
        "occurred_at": "2026-10-20T12:00:00Z",
        "agent_id": str(uuid4()),
        "agent_name": "Test Agent",
        "agent_version": "1.0",
        "task_name": "Provenance test",
    }

    messages = [
        (
            "1729420000000-2",
            {
                "payload": json.dumps(event_dict),
                "source_partition": "not-an-int",
            },
        )
    ]

    await store_messages(
        redis=mock_redis,
        connection=mock_connection,
        stream="trace:persistence",
        group="postgres-writers",
        messages=messages,
    )

    # Malformed metadata should fail before commit and MUST NOT call xack
    assert mock_redis.xack.call_count == 0


@pytest.mark.anyio
async def test_persist_event_duplicate_id_is_idempotent() -> None:
    mock_connection = AsyncMock()
    mock_cursor = AsyncMock()
    # Duplicate returns None for RETURNING event_id due to ON CONFLICT DO NOTHING
    mock_cursor.fetchone.return_value = None
    mock_connection.execute.return_value = mock_cursor

    event = ExecutionStartedEvent(
        event_id=uuid4(),
        event_type="execution_started",
        execution_id=uuid4(),
        occurred_at="2026-10-20T12:00:00Z",
        agent_id=uuid4(),
        agent_name="Test Agent",
        agent_version="1.0.0",
        task_name="Task",
    )

    inserted = await persist_event(
        mock_connection,
        event,
        source_topic="trace-events",
        source_partition=0,
        source_offset=100,
    )

    assert inserted is False
    # Only the processed_events INSERT was attempted; no downstream inserts
    assert mock_connection.execute.call_count == 1

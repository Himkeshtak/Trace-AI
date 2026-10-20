import json
from typing import NamedTuple
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from consumer.worker import process_record, quarantine_record


class MockConsumerRecord(NamedTuple):
    topic: str
    partition: int
    offset: int
    value: bytes


def create_mock_record(
    value: bytes,
    topic: str = "trace-events",
    partition: int = 0,
    offset: int = 10,
) -> MockConsumerRecord:
    return MockConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        value=value,
    )


@pytest.mark.anyio
async def test_plain_text_payload_is_quarantined() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    record = create_mock_record(b"traceai-vv-test-plain-text")

    handled = await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled is True
    assert mock_hot_store.write.call_count == 0
    mock_redis.xadd.assert_awaited_once()
    call_args = mock_redis.xadd.call_args
    assert call_args[0][0] == "trace:quarantine"
    entry = call_args[0][1]
    assert entry["error_category"] == "JSONDecodeError"
    assert entry["topic"] == "trace-events"
    assert entry["offset"] == "10"
    assert entry["payload_preview"] == "traceai-vv-test-plain-text"


@pytest.mark.anyio
async def test_invalid_json_is_quarantined() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    record = create_mock_record(b'{"incomplete_json": ')

    handled = await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled is True
    assert mock_hot_store.write.call_count == 0
    mock_redis.xadd.assert_awaited_once()
    entry = mock_redis.xadd.call_args[0][1]
    assert entry["error_category"] == "JSONDecodeError"


@pytest.mark.anyio
async def test_invalid_utf8_is_quarantined() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    record = create_mock_record(b"\xff\xfe\xfd")

    handled = await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled is True
    assert mock_hot_store.write.call_count == 0
    mock_redis.xadd.assert_awaited_once()
    entry = mock_redis.xadd.call_args[0][1]
    assert entry["error_category"] == "UnicodeDecodeError"


@pytest.mark.anyio
async def test_schema_violation_is_quarantined() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    invalid_event_json = json.dumps({"event_type": "unknown_type", "event_id": str(uuid4())}).encode("utf-8")
    record = create_mock_record(invalid_event_json)

    handled = await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled is True
    assert mock_hot_store.write.call_count == 0
    mock_redis.xadd.assert_awaited_once()
    entry = mock_redis.xadd.call_args[0][1]
    assert entry["error_category"] == "ValidationError"


@pytest.mark.anyio
async def test_valid_event_is_processed() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    mock_hot_store.write.return_value = True

    valid_event = {
        "event_id": str(uuid4()),
        "event_type": "execution_started",
        "execution_id": str(uuid4()),
        "occurred_at": "2026-10-19T00:00:00Z",
        "agent_id": str(uuid4()),
        "agent_name": "Test Agent",
        "agent_version": "1.0.0",
        "task_name": "Task",
    }
    record = create_mock_record(json.dumps(valid_event).encode("utf-8"))

    handled = await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled is True
    assert mock_hot_store.write.call_count == 1
    assert mock_redis.xadd.call_count == 0
    call_kwargs = mock_hot_store.write.call_args[1]
    assert call_kwargs["topic"] == "trace-events"
    assert call_kwargs["partition"] == 0
    assert call_kwargs["offset"] == 10


@pytest.mark.anyio
async def test_malformed_followed_by_valid_record() -> None:
    mock_redis = AsyncMock()
    mock_hot_store = AsyncMock()
    mock_hot_store.write.return_value = True

    malformed_record = create_mock_record(b"corrupt-data", offset=1)
    valid_event = {
        "event_id": str(uuid4()),
        "event_type": "execution_started",
        "execution_id": str(uuid4()),
        "occurred_at": "2026-10-19T00:00:00Z",
        "agent_id": str(uuid4()),
        "agent_name": "Test Agent",
        "agent_version": "1.0.0",
        "task_name": "Task",
    }
    valid_record = create_mock_record(json.dumps(valid_event).encode("utf-8"), offset=2)

    handled1 = await process_record(malformed_record, mock_hot_store, mock_redis, "trace:quarantine")
    handled2 = await process_record(valid_record, mock_hot_store, mock_redis, "trace:quarantine")

    assert handled1 is True
    assert handled2 is True
    assert mock_redis.xadd.call_count == 1  # only first was quarantined
    assert mock_hot_store.write.call_count == 1  # second was stored


@pytest.mark.anyio
async def test_quarantine_storage_failure_raises() -> None:
    mock_redis = AsyncMock()
    mock_redis.xadd.side_effect = RuntimeError("Redis connection broken")
    mock_hot_store = AsyncMock()
    record = create_mock_record(b"corrupt-data")

    # If quarantine storage fails, it must raise so the caller does not commit the offset
    with pytest.raises(RuntimeError, match="Redis connection broken"):
        await process_record(record, mock_hot_store, mock_redis, "trace:quarantine")

    assert mock_hot_store.write.call_count == 0

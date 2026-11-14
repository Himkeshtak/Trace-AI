from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.structs import TopicPartition

from consumer import worker


class MockConsumerRecord(NamedTuple):
    topic: str
    partition: int
    offset: int
    value: bytes = b"{}"


def record(partition: int, offset: int) -> MockConsumerRecord:
    return MockConsumerRecord("trace-events", partition, offset)


def consumer(*partitions: TopicPartition) -> MagicMock:
    mock = MagicMock()
    mock.assignment.return_value = set(partitions)
    mock.commit = AsyncMock()
    mock.seek = MagicMock()
    return mock


def commit_offsets_requested(mock_consumer: MagicMock, partition: TopicPartition) -> list[int]:
    return [
        call.args[0][partition].offset
        for call in mock_consumer.commit.await_args_list
        if partition in call.args[0]
    ]


@pytest.mark.anyio
async def test_first_record_failure_seeks_and_stops_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tp = TopicPartition("trace-events", 0)
    mock_consumer = consumer(tp)
    process = AsyncMock(side_effect=RuntimeError("Redis unavailable"))
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {tp: [record(0, 10), record(0, 11)]},
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is True
    assert [call.args[0].offset for call in process.await_args_list] == [10]
    mock_consumer.commit.assert_not_awaited()
    mock_consumer.seek.assert_called_once_with(tp, 10)


@pytest.mark.anyio
async def test_middle_record_failure_keeps_only_prior_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tp = TopicPartition("trace-events", 0)
    mock_consumer = consumer(tp)
    process = AsyncMock(side_effect=[True, RuntimeError("Redis unavailable"), True])
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {tp: [record(0, 10), record(0, 11), record(0, 12)]},
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is True
    assert [call.args[0].offset for call in process.await_args_list] == [10, 11]
    assert commit_offsets_requested(mock_consumer, tp) == [11]
    mock_consumer.seek.assert_called_once_with(tp, 11)


@pytest.mark.anyio
async def test_all_records_succeed_commits_each_next_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tp = TopicPartition("trace-events", 0)
    mock_consumer = consumer(tp)
    process = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {tp: [record(0, 10), record(0, 11), record(0, 12)]},
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is False
    assert commit_offsets_requested(mock_consumer, tp) == [11, 12, 13]
    mock_consumer.seek.assert_not_called()


@pytest.mark.anyio
async def test_commit_failure_seeks_and_stops_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tp = TopicPartition("trace-events", 0)
    mock_consumer = consumer(tp)
    mock_consumer.commit.side_effect = RuntimeError("Kafka commit failed")
    process = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {tp: [record(0, 10), record(0, 11)]},
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is True
    assert [call.args[0].offset for call in process.await_args_list] == [10]
    assert commit_offsets_requested(mock_consumer, tp) == [11]
    mock_consumer.seek.assert_called_once_with(tp, 10)


@pytest.mark.anyio
async def test_failure_does_not_block_independent_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_tp = TopicPartition("trace-events", 0)
    healthy_tp = TopicPartition("trace-events", 1)
    mock_consumer = consumer(failed_tp, healthy_tp)
    process = AsyncMock(
        side_effect=[RuntimeError("Redis unavailable"), True, True]
    )
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {
            failed_tp: [record(0, 10), record(0, 11)],
            healthy_tp: [record(1, 20), record(1, 21)],
        },
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is True
    assert [call.args[0].offset for call in process.await_args_list] == [10, 20, 21]
    assert commit_offsets_requested(mock_consumer, failed_tp) == []
    assert commit_offsets_requested(mock_consumer, healthy_tp) == [21, 22]
    mock_consumer.seek.assert_called_once_with(failed_tp, 10)


@pytest.mark.anyio
async def test_revoked_partition_is_not_sought_or_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tp = TopicPartition("trace-events", 0)
    mock_consumer = consumer(tp)
    mock_consumer.assignment.side_effect = [{tp}, set()]
    process = AsyncMock(side_effect=RuntimeError("processing failed during rebalance"))
    monkeypatch.setattr(worker, "process_record", process)

    had_failure = await worker.process_batch(
        mock_consumer,
        {tp: [record(0, 10), record(0, 11)]},
        MagicMock(),
        MagicMock(),
        "trace:quarantine",
    )

    assert had_failure is True
    assert [call.args[0].offset for call in process.await_args_list] == [10]
    mock_consumer.commit.assert_not_awaited()
    mock_consumer.seek.assert_not_called()

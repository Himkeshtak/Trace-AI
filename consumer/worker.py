import asyncio
import json
import logging
import signal
from contextlib import suppress
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition
from pydantic import ValidationError
from redis.asyncio import Redis

from consumer.config import ConsumerSettings
from consumer.hot_store import RedisHotStore
from shared.traceai_contracts import trace_event_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("traceai.consumer")


async def quarantine_record(
    redis: Redis,
    stream: str,
    record: ConsumerRecord,
    error_category: str,
    error_message: str,
) -> None:
    raw_preview = record.value[:1000].decode("utf-8", errors="replace") if isinstance(record.value, (bytes, bytearray)) else str(record.value)[:1000]
    entry = {
        "topic": str(record.topic),
        "partition": str(record.partition),
        "offset": str(record.offset),
        "timestamp": datetime.now(UTC).isoformat(),
        "error_category": error_category,
        "error_message": error_message[:500],
        "payload_preview": raw_preview,
    }
    await redis.xadd(stream, entry, maxlen=10_000, approximate=True)


async def process_record(
    record: ConsumerRecord,
    hot_store: RedisHotStore,
    redis: Redis,
    quarantine_stream: str,
) -> bool:
    """Processes a single record.

    Returns True if record was handled (valid or quarantined) and offset can be committed.
    Raises exception on transient failure (e.g. Redis connection error) so offset is not committed.
    """
    raw_value = record.value
    if not isinstance(raw_value, (bytes, bytearray)):
        raw_value = str(raw_value).encode("utf-8")

    # 1. UTF-8 decode
    try:
        decoded_str = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        logger.warning("record_unicode_decode_failed offset=%s error=%s", record.offset, exc)
        await quarantine_record(redis, quarantine_stream, record, "UnicodeDecodeError", str(exc))
        return True

    # 2. JSON parse
    try:
        parsed_payload = json.loads(decoded_str)
    except json.JSONDecodeError as exc:
        logger.warning("record_json_decode_failed offset=%s error=%s", record.offset, exc)
        await quarantine_record(redis, quarantine_stream, record, "JSONDecodeError", str(exc))
        return True

    # 3. Pydantic validation
    try:
        event = trace_event_adapter.validate_python(parsed_payload)
    except ValidationError as exc:
        logger.warning("record_contract_validation_failed offset=%s error=%s", record.offset, exc)
        await quarantine_record(redis, quarantine_stream, record, "ValidationError", str(exc))
        return True

    # 4. Valid event - Hot store write
    inserted = await hot_store.write(
        event,
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
    )
    logger.info(
        "event_processed event_id=%s event_type=%s duplicate=%s",
        event.event_id,
        event.event_type,
        not inserted,
    )
    return True


async def consume() -> None:
    settings = ConsumerSettings()
    kafka_consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    hot_store = RedisHotStore(
        redis,
        settings.redis_persistence_stream,
        settings.redis_processed_event_ttl_seconds,
    )
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signal_name, stopping.set)

    await kafka_consumer.start()
    logger.info("consumer_started topic=%s group=%s", settings.kafka_topic, settings.kafka_consumer_group)
    try:
        while not stopping.is_set():
            batch = await kafka_consumer.getmany(timeout_ms=1_000, max_records=100)
            if not batch:
                continue
            for records in batch.values():
                for record in records:
                    try:
                        handled = await process_record(
                            record,
                            hot_store,
                            redis,
                            settings.redis_quarantine_stream,
                        )
                        if handled:
                            partition = TopicPartition(record.topic, record.partition)
                            await kafka_consumer.commit(
                                {partition: OffsetAndMetadata(record.offset + 1, "")}
                            )
                    except Exception:
                        logger.exception("event_processing_failed offset=%s", record.offset)
                        await asyncio.sleep(1)
    finally:
        await kafka_consumer.stop()
        await redis.aclose()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(consume())

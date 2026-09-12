import asyncio
import json
import logging
import signal
from contextlib import suppress

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from psycopg import AsyncConnection
from redis.asyncio import Redis

from consumer.config import ConsumerSettings
from consumer.handlers import persist_event
from shared.traceai_contracts import trace_event_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("traceai.consumer")


async def invalidate_analytics_cache(redis: Redis) -> None:
    keys = [key async for key in redis.scan_iter(match="analytics:*")]
    if keys:
        await redis.delete(*keys)


async def consume() -> None:
    settings = ConsumerSettings()
    kafka_consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    connection = await AsyncConnection.connect(settings.database_url)
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
                        event = trace_event_adapter.validate_python(record.value)
                        async with connection.transaction():
                            inserted = await persist_event(connection, event)
                        if inserted:
                            await invalidate_analytics_cache(redis)
                        partition = TopicPartition(record.topic, record.partition)
                        await kafka_consumer.commit(
                            {partition: OffsetAndMetadata(record.offset + 1, "")}
                        )
                        logger.info(
                            "event_processed event_id=%s event_type=%s duplicate=%s",
                            event.event_id,
                            event.event_type,
                            not inserted,
                        )
                    except Exception:
                        logger.exception("event_processing_failed offset=%s", record.offset)
                        await asyncio.sleep(1)
    finally:
        await kafka_consumer.stop()
        await redis.aclose()
        await connection.close()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(consume())

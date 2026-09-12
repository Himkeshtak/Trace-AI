import asyncio
import json
import logging
import socket
from collections.abc import Iterable
from typing import Any

from psycopg import AsyncConnection
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from persistence.config import PersistenceSettings
from persistence.handlers import persist_event
from shared.traceai_contracts import trace_event_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("traceai.persistence")


async def ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0-0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise


async def store_messages(
    redis: Redis,
    connection: AsyncConnection[Any],
    stream: str,
    group: str,
    messages: Iterable[tuple[str, dict[str, str]]],
) -> None:
    for message_id, fields in messages:
        try:
            event = trace_event_adapter.validate_python(json.loads(fields["payload"]))
            async with connection.transaction():
                inserted = await persist_event(connection, event)
            await redis.xack(stream, group, message_id)
            logger.info(
                "event_archived event_id=%s event_type=%s duplicate=%s",
                event.event_id,
                event.event_type,
                not inserted,
            )
        except Exception:
            logger.exception("archive_failed stream_id=%s", message_id)


async def run() -> None:
    settings = PersistenceSettings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    connection = await AsyncConnection.connect(settings.database_url)
    consumer_name = socket.gethostname()
    await ensure_group(redis, settings.redis_persistence_stream, settings.redis_persistence_group)
    logger.info(
        "persistence_started stream=%s group=%s",
        settings.redis_persistence_stream,
        settings.redis_persistence_group,
    )
    try:
        while True:
            claimed = await redis.xautoclaim(
                settings.redis_persistence_stream,
                settings.redis_persistence_group,
                consumer_name,
                min_idle_time=60_000,
                start_id="0-0",
                count=50,
            )
            if claimed[1]:
                await store_messages(
                    redis,
                    connection,
                    settings.redis_persistence_stream,
                    settings.redis_persistence_group,
                    claimed[1],
                )

            batches = await redis.xreadgroup(
                settings.redis_persistence_group,
                consumer_name,
                {settings.redis_persistence_stream: ">"},
                count=100,
                block=1_000,
            )
            for _, messages in batches:
                await store_messages(
                    redis,
                    connection,
                    settings.redis_persistence_stream,
                    settings.redis_persistence_group,
                    messages,
                )
    finally:
        await redis.aclose()
        await connection.close()


if __name__ == "__main__":
    asyncio.run(run())


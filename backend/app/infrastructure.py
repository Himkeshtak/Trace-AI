import json
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaProducer
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from backend.app.config import Settings
from shared.traceai_contracts import TraceEvent


class EventPublisher:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.kafka_topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, event: TraceEvent) -> None:
        await self._producer.send_and_wait(
            self._topic,
            key=str(event.execution_id),
            value=event.model_dump(mode="json"),
        )


class AnalyticsStore:
    def __init__(self, database_url: str) -> None:
        self.pool = AsyncConnectionPool(
            database_url,
            min_size=1,
            max_size=8,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    async def open(self) -> None:
        await self.pool.open(wait=True)

    async def close(self) -> None:
        await self.pool.close()

    async def fetch_one(self, query: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any]:
        async with self.pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, parameters)
                row = await cursor.fetchone()
                return dict(row) if row else {}

    async def fetch_all(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        async with self.pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, parameters)
                return [dict(row) for row in await cursor.fetchall()]

    async def ping(self) -> bool:
        return (await self.fetch_one("SELECT 1 AS ready")).get("ready") == 1


class AnalyticsCache:
    def __init__(self, redis_url: str, ttl_seconds: int) -> None:
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds

    async def close(self) -> None:
        await self.redis.aclose()

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def get_or_load(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, bool]:
        cached = await self.redis.get(key)
        if cached is not None:
            return json.loads(cached), True
        value = await loader()
        await self.redis.setex(key, self.ttl_seconds, json.dumps(value, default=str))
        return value, False


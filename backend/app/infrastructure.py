import json

from aiokafka import AIOKafkaProducer
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


class HotAnalyticsStore:
    def __init__(self, redis_url: str) -> None:
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    async def close(self) -> None:
        await self.redis.aclose()

    async def ping(self) -> bool:
        return bool(await self.redis.ping())


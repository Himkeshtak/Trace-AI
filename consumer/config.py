from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "trace-events"
    kafka_consumer_group: str = "traceai-storage"
    redis_url: str = "redis://localhost:6379/0"
    redis_persistence_stream: str = "trace:persistence"
    redis_processed_event_ttl_seconds: int = 604_800

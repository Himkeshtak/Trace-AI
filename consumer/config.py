from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://traceai:traceai_dev_password@localhost:5432/traceai"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "trace-events"
    kafka_consumer_group: str = "traceai-storage"
    redis_url: str = "redis://localhost:6379/0"


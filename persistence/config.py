from pydantic_settings import BaseSettings, SettingsConfigDict


class PersistenceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://traceai:traceai_dev_password@localhost:5432/traceai"
    redis_url: str = "redis://localhost:6379/0"
    redis_persistence_stream: str = "trace:persistence"
    redis_persistence_group: str = "postgres-writers"


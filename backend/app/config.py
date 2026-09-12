from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "TraceAI API"
    environment: str = "development"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "trace-events"
    redis_url: str = "redis://localhost:6379/0"
    api_cors_origins: str = "http://localhost:5173,http://localhost:3000"

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.api_cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()

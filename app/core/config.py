from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    MAX_QUEUE_SIZE: int = 100_000
    BATCH_SIZE_LIMIT: int = 5_000
    FLUSH_INTERVAL_SECONDS: float = 2.0

    # Required settings (Optional at import time to prevent crashes)
    POSTGRES_URL: Optional[str] = None
    MONGO_URI: Optional[str] = None
    MONGO_DB_NAME: str = "velicor"
    REDIS_URL: Optional[str] = None

    # Security
    JWT_SECRET_KEY: Optional[str] = None
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 1 week

    SERVERLESS_MODE: bool = False

    @property
    def is_serverless(self) -> bool:
        import os

        return self.SERVERLESS_MODE or os.environ.get("VERCEL") == "1"

    ALLOW_ORIGINS: list[str] = ["*"]
    CRON_SECRET: Optional[str] = None

    model_config = SettingsConfigDict(
        env_prefix="LOG_INGEST_", env_file=".env", extra="ignore"
    )


settings = Settings()

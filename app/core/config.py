from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MAX_QUEUE_SIZE: int = 100_000
    BATCH_SIZE_LIMIT: int = 5_000
    FLUSH_INTERVAL_SECONDS: float = 2.0

    # Required settings (Optional at import time to prevent crashes)
    POSTGRES_URL: str | None = None
    MONGO_URI: str | None = None
    MONGO_DB_NAME: str = "velicor"
    REDIS_URL: str | None = None

    # Security
    JWT_SECRET_KEY: str | None = None
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 1 week

    # S3 Archival
    S3_BUCKET_NAME: str | None = None
    S3_ACCESS_KEY_ID: str | None = None
    S3_SECRET_ACCESS_KEY: str | None = None
    S3_REGION_NAME: str = "us-east-1"

    SERVERLESS_MODE: bool = False
    ENVIRONMENT: str = "dev"

    @property
    def is_serverless(self) -> bool:
        import os

        return self.SERVERLESS_MODE or os.environ.get("VERCEL") == "1"

    ALLOW_ORIGINS: list[str] = ["*"]
    CRON_SECRET: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="LOG_INGEST_", env_file=".env", extra="ignore"
    )


settings = Settings()

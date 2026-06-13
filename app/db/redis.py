import logging
from typing import Optional
import redis.asyncio as aioredis
from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisManager:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        if not settings.REDIS_URL:
            logger.info("REDIS_URL is not set. Redis features will be disabled.")
            return

        if not self.client:
            try:
                # Use a connection pool for Redis
                self.client = aioredis.from_url(
                    settings.REDIS_URL,
                    encoding="utf-8",
                    decode_responses=True,
                    health_check_interval=30,
                    retry_on_timeout=True,
                )
                await self.client.ping()
                logger.info("Connected to Redis successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.client = None

    async def disconnect(self):
        if self.client:
            try:
                await self.client.aclose()
            except Exception as e:
                logger.error(f"Error during Redis close: {e}")
            self.client = None
            logger.info("Disconnected from Redis")


redis_manager = RedisManager()

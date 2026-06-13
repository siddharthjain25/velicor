import asyncio
import logging
import orjson
from typing import Any
from app.db.redis import redis_manager

logger = logging.getLogger(__name__)


class RedisPersistentQueue:
    """
    A drop-in replacement wrapper for asyncio.Queue that uses Redis LPUSH/BLPOP
    when Redis is available, falling back gracefully to the in-memory queue otherwise.
    """

    def __init__(
        self, fallback_queue: asyncio.Queue, redis_key: str = "velicor:ingest_queue"
    ):
        self.fallback_queue = fallback_queue
        self.redis_key = redis_key

    def put_nowait(self, item: Any):
        """Push item to the queue. Non-blocking."""
        if redis_manager.client:
            # Spawn a background task to push to Redis asynchronously
            asyncio.create_task(self._redis_push(item))
        else:
            self.fallback_queue.put_nowait(item)

    async def _redis_push(self, item: Any):
        try:
            await redis_manager.client.rpush(self.redis_key, orjson.dumps(item))
        except Exception as e:
            logger.error(
                f"Failed to push to Redis queue: {e}. Falling back to in-memory queue."
            )
            self.fallback_queue.put_nowait(item)

    async def get(self) -> Any:
        """Get item from the queue. Blocks if empty."""
        if redis_manager.client:
            while True:
                try:
                    # BLPOP blocks up to 1 second so we can check for cancellations/shutdowns
                    res = await redis_manager.client.blpop(self.redis_key, timeout=1)
                    if res:
                        return orjson.loads(res[1])
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        f"Redis queue pop error: {e}. Checking in-memory queue."
                    )
                    # If Redis fails, check if there are fallback memory items
                    if not self.fallback_queue.empty():
                        return await self.fallback_queue.get()
                    await asyncio.sleep(
                        1
                    )  # Prevent busy loop on Redis connection issues

        # Fallback to standard memory queue
        return await self.fallback_queue.get()

    def get_nowait(self) -> Any:
        """Get item from the fallback queue without blocking. Used for shutdown flushing."""
        return self.fallback_queue.get_nowait()

    def task_done(self):
        """Call task_done on the memory queue if applicable."""
        try:
            self.fallback_queue.task_done()
        except ValueError:
            pass

    def empty(self) -> bool:
        """Returns True if the memory queue is empty."""
        return self.fallback_queue.empty()

    def qsize(self) -> int:
        """Returns size of the memory queue."""
        return self.fallback_queue.qsize()

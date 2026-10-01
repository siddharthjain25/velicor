import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class PersistentQueue:
    """
    Wrapper for asyncio.Queue for log ingestion queuing.
    """

    def __init__(
        self, fallback_queue: asyncio.Queue
    ):
        self.fallback_queue = fallback_queue

    def put_nowait(self, item: Any):
        """Push item to the queue. Non-blocking."""
        self.fallback_queue.put_nowait(item)

    async def get(self) -> Any:
        """Get item from the queue. Blocks if empty."""
        return await self.fallback_queue.get()

    def get_nowait(self) -> Any:
        """Get item from the queue without blocking. Used for shutdown flushing."""
        return self.fallback_queue.get_nowait()

    def task_done(self):
        """Call task_done on the queue if applicable."""
        try:
            self.fallback_queue.task_done()
        except ValueError:
            pass

    def empty(self) -> bool:
        """Returns True if the queue is empty."""
        return self.fallback_queue.empty()

    def qsize(self) -> int:
        """Returns size of the queue."""
        return self.fallback_queue.qsize()

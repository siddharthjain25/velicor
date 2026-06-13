import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Any
from app.core.config import settings
from app.db.postgres import pg_manager

logger = logging.getLogger(__name__)


async def pipeline_worker(log_queue: asyncio.Queue):
    current_batch: List[Any] = []
    while True:
        try:
            try:
                item = await asyncio.wait_for(
                    log_queue.get(), timeout=settings.FLUSH_INTERVAL_SECONDS
                )
                current_batch.append(item)
                log_queue.task_done()
                if len(current_batch) >= settings.BATCH_SIZE_LIMIT:
                    await _trigger_flush(current_batch)
                    current_batch = []
            except asyncio.TimeoutError:
                if current_batch:
                    await _trigger_flush(current_batch)
                    current_batch = []
        except asyncio.CancelledError:
            if current_batch:
                await _trigger_flush(current_batch)
            raise
        except Exception as e:
            logger.error(f"Error in pipeline worker loop: {e}", exc_info=True)


async def _trigger_flush(batch: List[Any]):
    if not batch:
        return
    try:
        await pg_manager.insert_batch(batch)
        logger.info(f"Successfully flushed {len(batch)} logs to Postgres")
    except Exception as e:
        logger.critical(f"Pipeline flush to Postgres failed: {e}", exc_info=True)


async def flush_remaining(log_queue: asyncio.Queue):
    remaining_batch = []
    while not log_queue.empty():
        try:
            item = log_queue.get_nowait()
            remaining_batch.append(item)
            log_queue.task_done()
            if len(remaining_batch) >= settings.BATCH_SIZE_LIMIT:
                await _trigger_flush(remaining_batch)
                remaining_batch = []
        except asyncio.QueueEmpty:
            break
    if remaining_batch:
        await _trigger_flush(remaining_batch)

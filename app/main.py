import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.services.worker import pipeline_worker, flush_remaining
from app.api.v1.endpoints import router as v1_router, set_queue
from app.api.v1.auth import router as auth_router
from app.api.v1.services import router as services_router
from app.db.postgres import pg_manager
from app.db.mongo import mongo_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

queue: asyncio.Queue = asyncio.Queue(maxsize=settings.MAX_QUEUE_SIZE)

async def retention_worker():
    # Small delay to let system settle
    await asyncio.sleep(10)
    while True:
        try:
            logger.info("Running automated retention purge")
            all_services = await mongo_manager.db.services.find({}).to_list(None)
            for service in all_services:
                retention = service.get("retention_days", 30)
                await pg_manager.purge_old_logs(service["name"], retention)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Retention worker error: {e}", exc_info=True)
        
        # Wait 24 hours before next run
        await asyncio.sleep(86400)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pg_manager.connect()
    await mongo_manager.connect()
    set_queue(queue)
    
    worker_task = None
    retention_task = None
    if not settings.SERVERLESS_MODE:
        worker_task = asyncio.create_task(pipeline_worker(queue))
        retention_task = asyncio.create_task(retention_worker())
        logger.info("Background pipeline worker started")
    else:
        logger.info("Running in SERVERLESS MODE (Sync ingestion)")
        
    logger.info("Application started")
    yield
    
    if worker_task:
        worker_task.cancel()
        try: await worker_task
        except asyncio.CancelledError: pass
        await flush_remaining(queue)
        
    if retention_task:
        retention_task.cancel()
        try: await retention_task
        except asyncio.CancelledError: pass
        
    await pg_manager.disconnect()
    await mongo_manager.disconnect()
    logger.info("Application stopped")

app = FastAPI(title="Log Ingestion & Search Layer", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, 
    allow_origins=settings.ALLOW_ORIGINS, 
    allow_credentials=True if settings.ALLOW_ORIGINS != ["*"] else False, 
    allow_methods=["*"], 
    allow_headers=["*"]
)
app.include_router(v1_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1/auth")
app.include_router(services_router, prefix="/api/v1/services")

@app.get("/health")
async def health_check():
    return {"status": "ok", "queue_size": queue.qsize()}

import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.services.worker import pipeline_worker, flush_remaining
from app.api.v1.endpoints import router as v1_router, set_queue
from app.api.v1.auth import router as auth_router
from app.api.v1.services import router as services_router
from app.api.v1.webhooks import router as webhooks_router
from app.db.postgres import pg_manager
from app.db.mongo import mongo_manager
from app.db.redis import redis_manager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from app.services.queue import RedisPersistentQueue

_memory_queue = asyncio.Queue(maxsize=settings.MAX_QUEUE_SIZE)
queue = RedisPersistentQueue(_memory_queue)


async def retention_worker():
    # Small delay to let system settle
    await asyncio.sleep(10)
    while True:
        try:
            logger.info("Running automated retention purge")
            all_services = await mongo_manager.db.services.find({}).to_list(None)
            for service in all_services:
                retention = service.get("retention_days", 30)
                deleted_count = await pg_manager.purge_old_logs(
                    service["name"], retention
                )

                # Send webhook notification if configured
                webhooks_data = service.get("webhooks", [])
                if webhooks_data:
                    from app.models.service import WebhookConfig
                    from app.services.notifier import trigger_retention_webhooks

                    webhooks = [WebhookConfig(**w) for w in webhooks_data]
                    await trigger_retention_webhooks(
                        webhooks, service["name"], retention, deleted_count
                    )
        except asyncio.CancelledError:
            logger.info("Retention worker task cancelled")
            raise
        except Exception as e:
            logger.exception(f"Retention worker error: {e}", exc_info=True)

        # Wait 24 hours before next run
        await asyncio.sleep(86400)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await pg_manager.connect()
    await mongo_manager.connect()
    await redis_manager.connect()
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

    try:
        from app.api.v1.endpoints import manager

        await manager.close()

        tasks = []
        if worker_task:
            worker_task.cancel()
            tasks.append(worker_task)
        if retention_task:
            retention_task.cancel()
            tasks.append(retention_task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await flush_remaining(queue)
    finally:
        if not settings.is_serverless:
            await pg_manager.disconnect()
            await mongo_manager.disconnect()
            await redis_manager.disconnect()
            logger.info("Application stopped")
        else:
            await redis_manager.disconnect()
            logger.info("Serverless: Skipping disconnect to allow connection reuse")


app = FastAPI(title="Log Ingestion & Search Layer", lifespan=lifespan)

from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.warning(
        f"HTTP {exc.status_code} Error: {exc.detail} (Path: {request.url.path})"
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "HTTPException",
            "message": exc.detail,
            "statusCode": exc.status_code,
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Validation Error: {exc.errors()} (Path: {request.url.path})")
    return JSONResponse(
        status_code=400,
        content={
            "error": "ValidationError",
            "message": "The request payload or parameter format was invalid.",
            "details": exc.errors(),
            "statusCode": 400,
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(
        f"Unhandled system crash: {exc} (Path: {request.url.path})", exc_info=True
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "InternalServerError",
            "message": "An unexpected system error occurred on the server.",
            "statusCode": 500,
        },
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOW_ORIGINS,
    allow_credentials=True if settings.ALLOW_ORIGINS != ["*"] else False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(v1_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1/auth")
app.include_router(services_router, prefix="/api/v1/services")
app.include_router(webhooks_router, prefix="/api/v1/webhooks")


@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="https://velicor-ui.vercel.app")


@app.get("/health")
async def health_check():
    return {"status": "ok", "queue_size": queue.qsize()}

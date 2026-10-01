import asyncio
import logging
import time
from collections import defaultdict
from typing import Annotated, Any

import orjson
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Header,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)

from app.core.config import settings
from app.db.mongo import mongo_manager
from app.services.notifier import trigger_webhooks

logger = logging.getLogger(__name__)
router = APIRouter()

API_KEY_CACHE: dict[str, tuple] = {}
CACHE_TTL = 300


async def get_service_from_key(x_api_key: str) -> dict | None:
    # Check local memory cache
    now = time.time()
    if x_api_key in API_KEY_CACHE:
        service, expiry = API_KEY_CACHE[x_api_key]
        if now < expiry:
            return service

    if not mongo_manager.client:
        await mongo_manager.connect()

    service = await mongo_manager.db.services.find_one({"secret_key": x_api_key})
    if service:
        service["_id"] = str(service["_id"])

        # Combine service-specific webhooks with user's global webhooks
        from bson import ObjectId

        user = await mongo_manager.db.users.find_one(
            {"_id": ObjectId(service["user_id"])}
        )
        service_webhooks = service.get("webhooks", [])
        user_webhooks = user.get("webhooks", []) if user else []
        service["webhooks"] = service_webhooks + user_webhooks

        # Store in local memory cache
        API_KEY_CACHE[x_api_key] = (service, now + CACHE_TTL)
        return service
    return None


async def invalidate_service_cache(service_id: str | None = None):
    # Invalidate local memory cache
    if service_id:
        to_delete = [
            k for k, v in API_KEY_CACHE.items() if v[0].get("_id") == service_id
        ]
        for k in to_delete:
            del API_KEY_CACHE[k]
    else:
        API_KEY_CACHE.clear()


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, service_name: str):
        await websocket.accept()
        self.active_connections[service_name].append(websocket)

    def disconnect(self, websocket: WebSocket, service_name: str):
        if service_name in self.active_connections:
            if websocket in self.active_connections[service_name]:
                self.active_connections[service_name].remove(websocket)
            if not self.active_connections[service_name]:
                del self.active_connections[service_name]

    async def close(self):
        pass

    async def broadcast(self, message: dict | str, service_name: str):
        if isinstance(message, dict):
            data = orjson.dumps(message).decode("utf-8")
        else:
            data = message

        disconnected = []
        for connection in self.active_connections.get(service_name, []):
            try:
                await connection.send_text(data)
            except Exception:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect(conn, service_name)


manager = ConnectionManager()

ingestion_queue: Any = None
LAST_RETENTION_RUNS: dict[str, float] = {}


def set_queue(q: Any):
    global ingestion_queue
    ingestion_queue = q


@router.post("/ingest")
async def ingest_logs(
    payload: dict[str, Any] | list[dict[str, Any]],
    background_tasks: BackgroundTasks,
    x_api_key: Annotated[str | None, Header()] = None,
):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API Key")

    # Validate API Key and get service name
    service = await get_service_from_key(x_api_key)
    if not service:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    verified_service_name = service["name"]
    incoming = [payload] if isinstance(payload, dict) else payload
    valid_logs = []

    for log in incoming:
        if "level" not in log:
            logger.warning(f"Discarding log missing mandatory 'level' field: {log}")
            continue

        log["service_name"] = verified_service_name
        valid_logs.append(log)

        # Broadcast to Live UI
        if settings.is_serverless:
            await manager.broadcast(log, verified_service_name)
        else:
            asyncio.create_task(manager.broadcast(log, verified_service_name))

    if not valid_logs:
        return {"status": "ignored", "processed": 0}

    # Trigger webhooks
    if service.get("webhooks"):
        from app.models.service import WebhookConfig

        webhooks = [WebhookConfig(**w) for w in service["webhooks"]]
        if settings.is_serverless:
            await trigger_webhooks(webhooks, valid_logs)
        else:
            asyncio.create_task(trigger_webhooks(webhooks, valid_logs))

    if settings.is_serverless:
        # Synchronous flush for Vercel
        from app.db.postgres import pg_manager

        try:
            await pg_manager.insert_batch(valid_logs)
            return {"status": "created", "processed": len(valid_logs)}
        except Exception as e:
            logger.error(f"Serverless flush failed: {e}")
            raise HTTPException(status_code=500, detail="Persistence failure")
    else:
        # Async queue for long-running servers
        if ingestion_queue:
            for log in valid_logs:
                ingestion_queue.put_nowait(log)
        return {"status": "accepted", "processed": len(valid_logs)}


@router.get("/search")
async def search_logs(
    start_ts: str | None = None,
    end_ts: str | None = None,
    level: str | None = None,
    status_code: int | None = None,
    keyword: str | None = None,
    limit: int = 100,
    x_api_key: Annotated[str | None, Header()] = None,
):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API Key")

    service = await get_service_from_key(x_api_key)
    if not service:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    from app.db.postgres import pg_manager

    return await pg_manager.search(
        service_name=service["name"],
        start_ts=start_ts,
        end_ts=end_ts,
        level=level,
        status_code=status_code,
        keyword=keyword,
        limit=limit,
    )


@router.get("/search/archive")
async def search_archive_logs(
    start_ts: str,
    end_ts: str,
    level: str | None = None,
    keyword: str | None = None,
    limit: int = 100,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Query cold storage S3 archives directly using DuckDB."""
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API Key")

    service = await get_service_from_key(x_api_key)
    if not service:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    from app.services.archiver import search_archive

    return await search_archive(
        service_name=service["name"],
        start_ts=start_ts,
        end_ts=end_ts,
        level=level,
        keyword=keyword,
        limit=limit,
    )


@router.websocket("/live")
async def live_tail(websocket: WebSocket, api_key: str | None = None):
    if not api_key:
        await websocket.close(code=1008, reason="Missing api_key")
        return

    service = await get_service_from_key(api_key)
    if not service:
        await websocket.close(code=1008, reason="Invalid api_key")
        return

    service_name = service["name"]
    await manager.connect(websocket, service_name)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, service_name)


@router.api_route("/maintenance/retention", methods=["GET", "POST"])
async def trigger_retention(authorization: str | None = Header(None)):
    if settings.CRON_SECRET:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Unauthorized")
        token = authorization.split(" ")[1]
        if token != settings.CRON_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    db = mongo_manager.get_db()
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")

    from app.db.postgres import pg_manager
    from app.models.service import WebhookConfig
    from app.services.notifier import trigger_retention_webhooks

    services_cursor = db.services.find({})
    results = {}

    async for service in services_cursor:
        service_name = service.get("name")
        retention_minutes = service.get("retention_minutes")
        if retention_minutes is None:
            retention_minutes = service.get("retention_days", 30) * 1440

        try:
            deleted_count = await pg_manager.purge_old_logs(
                service_name, retention_minutes
            )
            results[service_name] = deleted_count

            service_webhooks = service.get("webhooks", [])
            user_webhooks = []
            if "user_id" in service:
                from bson import ObjectId

                user = await db.users.find_one({"_id": ObjectId(service["user_id"])})
                if user:
                    user_webhooks = user.get("webhooks", [])

            webhooks_data = service_webhooks + user_webhooks

            if webhooks_data and deleted_count > 0:
                webhooks = [WebhookConfig(**w) for w in webhooks_data]
                await trigger_retention_webhooks(
                    webhooks=webhooks,
                    service_name=service_name,
                    retention_minutes=retention_minutes,
                    deleted_count=deleted_count,
                )
        except Exception as e:
            logger.error(f"Error processing retention for {service_name}: {e}")
            results[service_name] = -1

    return {"status": "success", "purged": results}

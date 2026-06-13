from fastapi import (
    APIRouter,
    status,
    WebSocket,
    WebSocketDisconnect,
    Header,
    HTTPException,
)
from typing import List, Union, Optional, Dict, Any, Annotated
import asyncio
import orjson
import logging
import time
from collections import defaultdict
from app.db.mongo import mongo_manager
from app.db.redis import redis_manager
from app.core.config import settings
from app.services.notifier import trigger_webhooks

logger = logging.getLogger(__name__)
router = APIRouter()

API_KEY_CACHE = {}
CACHE_TTL = 300


async def get_service_from_key(x_api_key: str) -> Optional[dict]:
    # Try Redis cache first if available
    redis_client = redis_manager.client
    if redis_client:
        try:
            cached_data = await redis_client.get(f"velicor:apikey:{x_api_key}")
            if cached_data:
                return orjson.loads(cached_data)
        except Exception as e:
            logger.error(f"Error reading from Redis key cache: {e}")

    # Fall back to local memory cache
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

        # Store in Redis if available
        if redis_client:
            try:
                service_id = service["_id"]
                serialized = orjson.dumps(service).decode("utf-8")
                await redis_client.set(
                    f"velicor:apikey:{x_api_key}", serialized, ex=CACHE_TTL
                )
                await redis_client.set(
                    f"velicor:service_to_key:{service_id}", x_api_key, ex=CACHE_TTL
                )
            except Exception as e:
                logger.error(f"Error writing to Redis key cache: {e}")

        # Store in local memory cache
        API_KEY_CACHE[x_api_key] = (service, now + CACHE_TTL)
        return service
    return None


async def invalidate_service_cache(service_id: str = None):
    # Invalidate Redis cache if available
    redis_client = redis_manager.client
    if redis_client:
        try:
            if service_id:
                x_api_key = await redis_client.get(
                    f"velicor:service_to_key:{service_id}"
                )
                if x_api_key:
                    await redis_client.delete(f"velicor:apikey:{x_api_key}")
                    await redis_client.delete(f"velicor:service_to_key:{service_id}")
            else:
                # Clear all cached keys
                async for key in redis_client.scan_iter("velicor:*"):
                    await redis_client.delete(key)
        except Exception as e:
            logger.error(f"Error invalidating Redis cache: {e}")

    # Invalidate local memory cache
    global API_KEY_CACHE
    if service_id:
        to_delete = [
            k for k, v in API_KEY_CACHE.items() if v[0].get("_id") == service_id
        ]
        for k in to_delete:
            del API_KEY_CACHE[k]
    else:
        API_KEY_CACHE.clear()


async def redis_websocket_subscriber():
    while True:
        try:
            redis_client = redis_manager.client
            if not redis_client:
                await asyncio.sleep(5)
                continue

            pubsub = redis_client.pubsub()
            await pubsub.psubscribe("velicor:pubsub:*")
            logger.info("Subscribed to Redis Pub/Sub pattern: velicor:pubsub:*")

            async for message in pubsub.listen():
                if message["type"] == "pmessage":
                    channel = message["channel"]
                    service_name = channel.split("velicor:pubsub:")[-1]
                    data = message["data"]
                    await manager.broadcast_local(data, service_name)
        except asyncio.CancelledError:
            logger.info("Redis Pub/Sub subscriber cancelled")
            break
        except Exception as e:
            logger.error(f"Error in Redis Pub/Sub WebSocket subscriber: {e}")
            await asyncio.sleep(5)


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = defaultdict(list)
        self.pubsub_task: Optional[asyncio.Task] = None

    async def connect(self, websocket: WebSocket, service_name: str):
        await websocket.accept()
        self.active_connections[service_name].append(websocket)

        # Dynamically start Redis Pub/Sub subscriber if not running
        if redis_manager.client and not self.pubsub_task:
            self.pubsub_task = asyncio.create_task(redis_websocket_subscriber())
            logger.info("Dynamically started Redis Pub/Sub WebSocket subscriber")

    def disconnect(self, websocket: WebSocket, service_name: str):
        if service_name in self.active_connections:
            if websocket in self.active_connections[service_name]:
                self.active_connections[service_name].remove(websocket)
            if not self.active_connections[service_name]:
                del self.active_connections[service_name]

        # Dynamically stop Redis Pub/Sub subscriber if no connections remain
        if not self.active_connections and self.pubsub_task:
            self.pubsub_task.cancel()
            self.pubsub_task = None
            logger.info("Dynamically stopped Redis Pub/Sub WebSocket subscriber")

    async def close(self):
        if self.pubsub_task:
            self.pubsub_task.cancel()
            self.pubsub_task = None
            logger.info("Closed Redis Pub/Sub WebSocket subscriber")

    async def broadcast_local(self, message: Union[dict, str], service_name: str):
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

    async def broadcast(self, message: dict, service_name: str):
        redis_client = redis_manager.client
        if redis_client:
            try:
                data = orjson.dumps(message).decode("utf-8")
                await redis_client.publish(f"velicor:pubsub:{service_name}", data)
            except Exception as e:
                logger.error(f"Failed to publish to Redis Pub/Sub: {e}")
                await self.broadcast_local(message, service_name)
        else:
            await self.broadcast_local(message, service_name)


manager = ConnectionManager()

ingestion_queue: Optional[asyncio.Queue] = None


def set_queue(q: asyncio.Queue):
    global ingestion_queue
    ingestion_queue = q


@router.post("/ingest")
async def ingest_logs(
    payload: Union[Dict[str, Any], List[Dict[str, Any]]],
    x_api_key: Annotated[Optional[str], Header()] = None,
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
    if "webhooks" in service and service["webhooks"]:
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
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    level: Optional[str] = None,
    status_code: Optional[int] = None,
    keyword: Optional[str] = None,
    limit: int = 100,
    x_api_key: Annotated[Optional[str], Header()] = None,
):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API Key")

    service = await get_service_from_key(x_api_key)
    if not service:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    from app.db.postgres import pg_manager

    results = await pg_manager.search(
        service_name=service["name"],
        start_ts=start_ts,
        end_ts=end_ts,
        level=level,
        status_code=status_code,
        keyword=keyword,
        limit=limit,
    )
    return results


@router.websocket("/live")
async def live_tail(websocket: WebSocket, api_key: Optional[str] = None):
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

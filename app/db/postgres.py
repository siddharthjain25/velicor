import asyncpg
import logging
import orjson
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.known_tables = set()

    async def connect(self):
        if not settings.POSTGRES_URL:
            logger.error("POSTGRES_URL is not set. Database features will be disabled.")
            return

        if not self.pool:
            # Use property for auto-detection
            is_sl = settings.is_serverless
            pool_size = 2 if is_sl else 10
            
            self.pool = await asyncpg.create_pool(
                settings.POSTGRES_URL,
                min_size=1,
                max_size=pool_size,
                statement_cache_size=0,
                ssl="require"
            )
            logger.info(f"Connected to Postgres (pool_size={pool_size}, mode={'Serverless' if is_sl else 'Persistent'})")

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Disconnected from Postgres")

    def _get_table_name(self, service_name: str) -> str:
        safe_name = "".join(c if c.isalnum() else "_" for c in service_name).lower()
        return f"logs_{safe_name}"

    async def ensure_table(self, table_name: str):
        if table_name in self.known_tables:
            return

        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    level VARCHAR(10) NOT NULL,
                    status_code INTEGER,
                    message TEXT,
                    metadata JSONB
                );
                CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name} (timestamp);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_status ON {table_name} (status_code);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_metadata ON {table_name} USING GIN (metadata);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_message_fts ON {table_name} USING GIN (to_tsvector('english', message));
            """)
        self.known_tables.add(table_name)

    async def insert_batch(self, batch: List[Dict[str, Any]]):
        if not self.pool: await self.connect()
        if not self.pool: return
        
        groups = {}
        for data in batch:
            service = data.get("service_name", "unknown")
            table = self._get_table_name(service)
            if table not in groups: groups[table] = []
            
            # Extract and parse timestamp
            ts_raw = data.get("timestamp")
            ts = None
            if ts_raw:
                try:
                    if isinstance(ts_raw, str):
                        ts = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                    elif isinstance(ts_raw, (int, float)):
                        ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
                    else:
                        ts = ts_raw
                except Exception:
                    ts = datetime.now(timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Ensure ts has timezone info for TIMESTAMPTZ
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            level = data.get("level")
            status_code = data.get("status_code")
            msg = data.get("message", "")
            
            # Everything else goes into metadata
            metadata = data.get("metadata", {})
            reserved = {"service_name", "timestamp", "level", "message", "metadata", "status_code"}
            extra = {k: v for k, v in data.items() if k not in reserved}
            if extra:
                metadata.update(extra)
            
            groups[table].append((
                ts,
                level,
                status_code,
                msg,
                orjson.dumps(metadata).decode("utf-8")
            ))

        for table_name, records in groups.items():
            await self.ensure_table(table_name)
            async with self.pool.acquire() as conn:
                await conn.executemany(f"""
                    INSERT INTO {table_name} (timestamp, level, status_code, message, metadata)
                    VALUES ($1, $2, $3, $4, $5)
                """, records)

    async def search(self, service_name: str, start_ts: Optional[str] = None, end_ts: Optional[str] = None, level: Optional[str] = None, status_code: Optional[int] = None, keyword: Optional[str] = None, limit: int = 100):
        if not self.pool: await self.connect()
        if not self.pool: return []
        
        table_name = self._get_table_name(service_name)
        await self.ensure_table(table_name)

        query = f"SELECT timestamp, level, status_code, message, metadata FROM {table_name} WHERE TRUE"
        args = []
        arg_idx = 1

        if start_ts:
            query += f" AND timestamp >= ${arg_idx}"
            args.append(datetime.fromisoformat(start_ts.replace('Z', '+00:00')))
            arg_idx += 1
        
        if end_ts:
            query += f" AND timestamp <= ${arg_idx}"
            args.append(datetime.fromisoformat(end_ts.replace('Z', '+00:00')))
            arg_idx += 1

        if level:
            query += f" AND level = ${arg_idx}"
            args.append(level)
            arg_idx += 1

        if status_code is not None:
            query += f" AND status_code = ${arg_idx}"
            args.append(status_code)
            arg_idx += 1

        if keyword:
            query += f" AND (to_tsvector('english', message) @@ plainto_tsquery('english', ${arg_idx}) OR metadata::text ILIKE ${arg_idx + 1})"
            args.append(keyword)
            args.append(f"%{keyword}%")
            arg_idx += 2

        query += f" ORDER BY timestamp DESC LIMIT ${arg_idx}"
        args.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [
                {
                    "timestamp": row["timestamp"].isoformat(),
                    "level": row["level"],
                    "status_code": row["status_code"],
                    "message": row["message"],
                    "metadata": orjson.loads(row["metadata"]) if row["metadata"] else {}
                }
                for row in rows
            ]

    async def delete_table(self, service_name: str):
        if not self.pool: await self.connect()
        if not self.pool: return
        
        table_name = self._get_table_name(service_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        if table_name in self.known_tables:
            self.known_tables.remove(table_name)
        logger.info(f"Dropped table {table_name} for service {service_name}")

    async def purge_old_logs(self, service_name: str, retention_days: int):
        if not self.pool: await self.connect()
        if not self.pool: return
        
        if retention_days <= 0:
            return
            
        table_name = self._get_table_name(service_name)
        async with self.pool.acquire() as conn:
            # Check if table exists before trying to delete
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = $1
                )
            """, table_name)
            
            if exists:
                result = await conn.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE timestamp < NOW() - INTERVAL '{retention_days} days'
                """)
                logger.info(f"Purged old logs from {table_name}: {result}")

    async def get_stats(self, service_name: str, interval_hours: int = 24):
        if not self.pool: await self.connect()
        if not self.pool: return {"levels": {}, "series": []}
        
        table_name = self._get_table_name(service_name)
        await self.ensure_table(table_name)
        
        async with self.pool.acquire() as conn:
            # Get counts by level
            level_counts = await conn.fetch(f"""
                SELECT level, COUNT(*) as count 
                FROM {table_name} 
                WHERE timestamp >= NOW() - INTERVAL '{interval_hours} hours'
                GROUP BY level
            """)
            
            # Get counts over time (buckets of 1 hour if > 24h, else 10 mins)
            bucket = "1 hour" if interval_hours > 24 else "10 minutes"
            time_series = await conn.fetch(f"""
                SELECT date_trunc('minute', timestamp) as bucket, COUNT(*) as count
                FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '{interval_hours} hours'
                GROUP BY bucket
                ORDER BY bucket ASC
            """)
            
            return {
                "levels": {row["level"]: row["count"] for row in level_counts},
                "series": [
                    {"timestamp": row["bucket"].isoformat(), "count": row["count"]}
                    for row in time_series
                ]
            }

pg_manager = PostgresManager()

import asyncpg
import logging
import orjson
import re
from datetime import datetime, timezone, date, timedelta
from typing import List, Dict, Any, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.known_tables = set()
        self.known_partitions = set()

    async def connect(self):
        if not settings.POSTGRES_URL:
            logger.error("POSTGRES_URL is not set. Database features will be disabled.")
            return

        # For Serverless, we prefer direct connections that we close manually
        # to ensure the proxy (Supavisor) doesn't run out of sessions.
        if settings.is_serverless:
            return

        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    settings.POSTGRES_URL,
                    min_size=1,
                    max_size=10,
                    statement_cache_size=0,
                    ssl="require"
                )
                logger.info("Connected to Postgres (Pool Mode)")
            except Exception as e:
                logger.error(f"Failed to create Postgres pool: {e}")
                self.pool = None
                raise e

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Disconnected from Postgres")

    async def get_connection(self):
        """Helper to get a connection from pool OR a new direct one for serverless."""
        if settings.is_serverless:
            return await asyncpg.connect(settings.POSTGRES_URL, ssl="require", statement_cache_size=0)
        
        if not self.pool:
            await self.connect()
        return self.pool.acquire()

    async def release_connection(self, conn):
        """Helper to release connection to pool OR close it for serverless."""
        if settings.is_serverless:
            await conn.close()
        else:
            await self.pool.release(conn)

    def _get_table_name(self, service_name: str) -> str:
        safe_name = "".join(c if c.isalnum() else "_" for c in service_name).lower()
        return f"logs_{safe_name}"

    async def ensure_table(self, table_name: str, conn: asyncpg.Connection):
        if table_name in self.known_tables:
            return

        async with conn.transaction():
            # Query pg_class to check if table exists and if it is partitioned
            class_info = await conn.fetchrow("""
                SELECT relkind FROM pg_class 
                JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid 
                WHERE relname = $1 AND nspname = 'public'
            """, table_name)

            if class_info:
                relkind = class_info["relkind"]
                if isinstance(relkind, bytes):
                    relkind = relkind.decode("utf-8")
                if relkind == 'p':
                    self.known_tables.add(table_name)
                    return
                elif relkind == 'r':
                    logger.info(f"Table {table_name} exists but is not partitioned. Migrating to partitioned schema...")
                    # Rename the existing table
                    await conn.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old")
                    
                    # Drop old indexes to prevent name conflicts during index creation on the new table
                    await conn.execute(f"""
                        DROP INDEX IF EXISTS idx_{table_name}_ts;
                        DROP INDEX IF EXISTS idx_{table_name}_status;
                        DROP INDEX IF EXISTS idx_{table_name}_metadata;
                        DROP INDEX IF EXISTS idx_{table_name}_message_fts;
                    """)
                    
                    # Create the new partitioned table
                    await self._create_partitioned_parent_table(table_name, conn)
                    
                    # Copy data from old to new
                    try:
                        await conn.execute(f"""
                            INSERT INTO {table_name} (timestamp, level, status_code, message, metadata) 
                            SELECT timestamp, level, status_code, message, metadata 
                            FROM {table_name}_old
                        """)
                        logger.info(f"Successfully migrated data for {table_name}")
                        await conn.execute(f"DROP TABLE {table_name}_old")
                    except Exception as e:
                        logger.error(f"Failed to copy data from {table_name}_old to {table_name}: {e}")
                        # Revert renaming if something went wrong
                        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        await conn.execute(f"ALTER TABLE {table_name}_old RENAME TO {table_name}")
                        raise e
                    self.known_tables.add(table_name)
                    return

            # Table does not exist, create it as a partitioned table
            await self._create_partitioned_parent_table(table_name, conn)
            self.known_tables.add(table_name)

    async def _create_partitioned_parent_table(self, table_name: str, conn: asyncpg.Connection):
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL,
                timestamp TIMESTAMPTZ NOT NULL,
                level VARCHAR(10) NOT NULL,
                status_code INTEGER,
                message TEXT,
                metadata JSONB,
                PRIMARY KEY (id, timestamp)
            ) PARTITION BY RANGE (timestamp);
            
            CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name} (timestamp);
            CREATE INDEX IF NOT EXISTS idx_{table_name}_status ON {table_name} (status_code);
            CREATE INDEX IF NOT EXISTS idx_{table_name}_metadata ON {table_name} USING GIN (metadata);
            CREATE INDEX IF NOT EXISTS idx_{table_name}_message_fts ON {table_name} USING GIN (to_tsvector('english', message));
            ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;
            
            -- Create default partition for safety/fallback
            CREATE TABLE IF NOT EXISTS {table_name}_default PARTITION OF {table_name} DEFAULT;
        """)

    async def insert_batch(self, batch: List[Dict[str, Any]]):
        if not settings.POSTGRES_URL: return
        
        conn = await self.get_connection()
        try:
            groups = {}
            for data in batch:
                service = data.get("service_name", "unknown")
                table = self._get_table_name(service)
                if table not in groups: groups[table] = []
                
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

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                level = data.get("level")
                status_code = data.get("status_code")
                msg = data.get("message", "")
                
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
                await self.ensure_table(table_name, conn)
                
                # Ensure all required daily partitions exist
                unique_dates = {rec[0].date() for rec in records}
                for d in unique_dates:
                    await self.ensure_partition(table_name, d, conn)

                await conn.executemany(f"""
                    INSERT INTO {table_name} (timestamp, level, status_code, message, metadata)
                    VALUES ($1, $2, $3, $4, $5)
                """, records)
        finally:
            await self.release_connection(conn)

    async def search(self, service_name: str, start_ts: Optional[str] = None, end_ts: Optional[str] = None, level: Optional[str] = None, status_code: Optional[int] = None, keyword: Optional[str] = None, limit: int = 100):
        if not settings.POSTGRES_URL: return []
        
        conn = await self.get_connection()
        try:
            table_name = self._get_table_name(service_name)
            await self.ensure_table(table_name, conn)

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
        finally:
            await self.release_connection(conn)

    async def delete_table(self, service_name: str):
        if not settings.POSTGRES_URL: return
        conn = await self.get_connection()
        try:
            table_name = self._get_table_name(service_name)
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            if table_name in self.known_tables:
                self.known_tables.remove(table_name)
            self.known_partitions = {k for k in self.known_partitions if not k.startswith(f"{table_name}:")}
            logger.info(f"Dropped table {table_name} for service {service_name}")
        finally:
            await self.release_connection(conn)

    async def ensure_partition(self, parent_table: str, day: date, conn: asyncpg.Connection):
        partition_suffix = day.strftime("y%Ym%md%d")
        cache_key = f"{parent_table}:{partition_suffix}"
        if cache_key in self.known_partitions:
            return
            
        partition_name = f"{parent_table}_{partition_suffix}"
        start_date = day.strftime("%Y-%m-%d")
        next_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
        
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition_name} 
            PARTITION OF {parent_table} 
            FOR VALUES FROM ('{start_date} 00:00:00+00') TO ('{next_date} 00:00:00+00');
        """)
        self.known_partitions.add(cache_key)

    async def purge_old_logs(self, service_name: str, retention_days: int) -> int:
        if not settings.POSTGRES_URL: return 0
        if retention_days <= 0: return 0
            
        conn = await self.get_connection()
        try:
            table_name = self._get_table_name(service_name)
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = $1
                )
            """, table_name)
            
            if not exists:
                return 0

            # Find all partition tables inheriting from the parent table
            partitions = await conn.fetch("""
                SELECT child.relname AS partition_name
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname = $1
            """, table_name)
            
            deleted_count = 0
            cutoff_date = date.today() - timedelta(days=retention_days)
            partition_re = re.compile(r".*_y(\d{4})m(\d{2})d(\d{2})$")
            
            for row in partitions:
                p_name = row["partition_name"]
                match = partition_re.match(p_name)
                if match:
                    p_year, p_month, p_day = map(int, match.groups())
                    p_date = date(p_year, p_month, p_day)
                    
                    if p_date < cutoff_date:
                        # Count rows to report exact deletion metrics
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {p_name}")
                        deleted_count += (count or 0)
                        
                        await conn.execute(f"DROP TABLE {p_name}")
                        logger.info(f"Dropped expired log partition table: {p_name}")
                        
                        # Remove from local partition cache
                        cache_key = p_name.replace(f"{table_name}_", f"{table_name}:")
                        if cache_key in self.known_partitions:
                            self.known_partitions.remove(cache_key)
                elif p_name == f"{table_name}_default":
                    # Clean up old logs from the default partition (fallback safety net)
                    result = await conn.execute(f"""
                        DELETE FROM {p_name} 
                        WHERE timestamp < NOW() - INTERVAL '{retention_days} days'
                    """)
                    if result and result.startswith("DELETE "):
                        try:
                            deleted_count += int(result.split(" ")[1])
                        except (IndexError, ValueError):
                            pass

            return deleted_count
        finally:
            await self.release_connection(conn)

    async def get_stats(self, service_name: str, interval_hours: int = 24):
        if not settings.POSTGRES_URL: return {"levels": {}, "series": []}
        
        conn = await self.get_connection()
        try:
            table_name = self._get_table_name(service_name)
            await self.ensure_table(table_name, conn)
            
            level_counts = await conn.fetch(f"""
                SELECT level, COUNT(*) as count 
                FROM {table_name} 
                WHERE timestamp >= NOW() - INTERVAL '{interval_hours} hours'
                GROUP BY level
            """)
            
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
        finally:
            await self.release_connection(conn)

pg_manager = PostgresManager()

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any

import boto3
import duckdb

from app.core.config import settings

logger = logging.getLogger(__name__)


async def archive_partition(
    conn,
    p_name: str,
    service_name: str,
    partition_date_str: str,
    cutoff_time: datetime | None = None,
) -> bool:
    if not settings.S3_BUCKET_NAME:
        return False

    logger.info(f"Archiving partition {p_name} to S3...")

    # Fetch rows from the partition
    query = (
        f"SELECT timestamp, level, status_code, message, metadata::text FROM {p_name}"  # nosec B608
    )
    if cutoff_time:
        query += f" WHERE timestamp < '{cutoff_time.isoformat()}'"  # nosec B608

    try:
        rows = await conn.fetch(query)
    except Exception as e:
        logger.error(f"Failed to fetch rows from {p_name} for archival: {e}")
        return False

    if not rows:
        logger.info(f"No rows found in {p_name}, skipping archival.")
        return True

    # Convert to list of dicts for DuckDB
    data = []
    for row in rows:
        data.append(
            {
                "timestamp": row["timestamp"].isoformat(),
                "level": row["level"],
                "status_code": row["status_code"],
                "message": row["message"] or "",
                "metadata": row["metadata"] or "{}",
            }
        )

    tmp_dir = tempfile.gettempdir()
    local_parquet_path = os.path.join(tmp_dir, f"{p_name}.parquet")
    local_jsonl_path = os.path.join(tmp_dir, f"{p_name}.jsonl")

    # Write to temporary JSONL file
    try:
        with open(local_jsonl_path, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
    except Exception as e:
        logger.error(f"Failed to write temporary JSONL: {e}")
        return False

    # Write to Parquet using DuckDB
    con = None
    try:
        con = duckdb.connect()
        con.execute(f"SET home_directory='{tmp_dir}';")
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{local_jsonl_path}')) TO '{local_parquet_path}' (FORMAT PARQUET)"  # nosec B608
        )
    except Exception as e:
        logger.error(f"Failed to generate parquet file: {e}")
        return False
    finally:
        if con:
            con.close()
        if os.path.exists(local_jsonl_path):
            os.remove(local_jsonl_path)

    # Upload to S3
    if cutoff_time:
        s3_key = f"{service_name}/{partition_date_str}_{int(cutoff_time.timestamp())}.parquet"
    else:
        s3_key = f"{service_name}/{partition_date_str}.parquet"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
            region_name=settings.S3_REGION_NAME,
        )
        s3.upload_file(local_parquet_path, settings.S3_BUCKET_NAME, s3_key)
        logger.info(
            f"Successfully archived {p_name} to s3://{settings.S3_BUCKET_NAME}/{s3_key}"
        )
    except Exception as e:
        logger.error(f"Failed to upload {local_parquet_path} to S3: {e}")
        return False
    finally:
        if os.path.exists(local_parquet_path):
            os.remove(local_parquet_path)

    return True


async def search_archive(
    service_name: str,
    start_ts: str,
    end_ts: str,
    level: str | None = None,
    keyword: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not settings.S3_BUCKET_NAME:
        logger.error("S3_BUCKET_NAME not configured")
        return []

    con = None
    try:
        con = duckdb.connect()
        # Fix missing HOME dir in serverless by pointing it to /tmp
        tmp_dir = tempfile.gettempdir()
        con.execute(f"SET home_directory='{tmp_dir}';")

        # Load AWS extensions
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Configure AWS credentials for DuckDB
        if settings.S3_ACCESS_KEY_ID:
            con.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{settings.S3_ACCESS_KEY_ID}',
                    SECRET '{settings.S3_SECRET_ACCESS_KEY}',
                    REGION '{settings.S3_REGION_NAME}'
                );
            """)

        # Build query using wildcard
        # DuckDB automatically uses Parquet min/max statistics to skip scanning irrelevant files!
        s3_path = f"s3://{settings.S3_BUCKET_NAME}/{service_name}/*.parquet"

        query = f"SELECT * FROM read_parquet('{s3_path}') WHERE timestamp >= '{start_ts}' AND timestamp <= '{end_ts}'"  # nosec B608

        if level:
            query += f" AND level = '{level}'"
        if keyword:
            # Prevent SQL injection loosely
            safe_keyword = keyword.replace("'", "''")
            query += f" AND (message ILIKE '%{safe_keyword}%' OR metadata ILIKE '%{safe_keyword}%')"

        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        # Execute query
        results = con.execute(query).fetchall()

        # Format results
        columns = [desc[0] for desc in con.description]
        formatted = []
        for row in results:
            item = dict(zip(columns, row))
            # Parse metadata back to dict
            try:
                item["metadata"] = json.loads(item.get("metadata", "{}"))
            except Exception:
                item["metadata"] = {}
            formatted.append(item)

        return formatted
    except Exception as e:
        # Ignore missing file errors if bucket is empty or no partitions archived yet
        if "No files found that match the pattern" in str(e):
            return []
        logger.error(f"DuckDB search archive error: {e}")
        return []
    finally:
        if con:
            con.close()

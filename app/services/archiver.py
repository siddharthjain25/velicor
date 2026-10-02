import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import duckdb

from app.core.config import settings

logger = logging.getLogger(__name__)


def _get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    tmp_dir = tempfile.gettempdir()
    con.execute(f"SET home_directory='{tmp_dir}';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if settings.S3_ACCESS_KEY_ID:
        con.execute(
            f"""
            CREATE SECRET aws_secret (
                TYPE S3,
                KEY_ID '{settings.S3_ACCESS_KEY_ID}',
                SECRET '{settings.S3_SECRET_ACCESS_KEY}',
                REGION '{settings.S3_REGION_NAME}'
            );
        """
        )
    return con


async def upload_batch_to_s3(logs: list[dict[str, Any]]) -> bool:
    """Group logs by service and date, write to Parquet, and upload directly to S3."""
    if not settings.S3_BUCKET_NAME or not logs:
        return False

    # Group logs by service_name and date
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for log in logs:
        service_name = log.get("service_name", "default")
        ts_raw = log.get("timestamp")
        try:
            if isinstance(ts_raw, str):
                dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            elif isinstance(ts_raw, (int, float)):
                dt = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
            else:
                dt = datetime.now(timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)

        date_str = dt.strftime("%Y-%m-%d")
        key = (service_name, date_str)
        if key not in grouped:
            grouped[key] = []

        metadata_val = log.get("metadata", {})
        metadata_str = (
            json.dumps(metadata_val)
            if isinstance(metadata_val, (dict, list))
            else str(metadata_val)
        )

        grouped[key].append(
            {
                "timestamp": dt.isoformat(),
                "level": str(log.get("level", "INFO")).upper(),
                "status_code": log.get("status_code"),
                "message": str(log.get("message", "")),
                "metadata": metadata_str,
            }
        )

    tmp_dir = tempfile.gettempdir()
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.S3_ACCESS_KEY_ID,
        aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
        region_name=settings.S3_REGION_NAME,
    )

    for (service_name, date_str), items in grouped.items():
        batch_id = f"{int(time.time() * 1000)}"
        file_prefix = f"log_{service_name}_{date_str}_{batch_id}"
        jsonl_path = os.path.join(tmp_dir, f"{file_prefix}.jsonl")
        parquet_path = os.path.join(tmp_dir, f"{file_prefix}.parquet")

        try:
            with open(jsonl_path, "w") as f:
                for item in items:
                    f.write(json.dumps(item) + "\n")

            con = duckdb.connect()
            con.execute(f"SET home_directory='{tmp_dir}';")
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{jsonl_path}')) TO '{parquet_path}' (FORMAT PARQUET)"
            )
            con.close()

            s3_key = f"{service_name}/{date_str}_{batch_id}.parquet"
            s3_client.upload_file(parquet_path, settings.S3_BUCKET_NAME, s3_key)
            logger.info(
                f"Uploaded {len(items)} logs to s3://{settings.S3_BUCKET_NAME}/{s3_key}"
            )
        except Exception as e:
            logger.error(f"Failed to process and upload S3 batch for {service_name}: {e}")
        finally:
            if os.path.exists(jsonl_path):
                os.remove(jsonl_path)
            if os.path.exists(parquet_path):
                os.remove(parquet_path)

    return True


async def search_s3_logs(
    service_name: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    level: str | None = None,
    status_code: int | None = None,
    keyword: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Search logs directly from S3 Parquet files using DuckDB."""
    if not settings.S3_BUCKET_NAME:
        logger.error("S3_BUCKET_NAME not configured")
        return []

    con = None
    try:
        con = _get_duckdb_connection()
        s3_path = f"s3://{settings.S3_BUCKET_NAME}/{service_name}/*.parquet"
        query = f"SELECT * FROM read_parquet('{s3_path}') WHERE 1=1"

        if start_ts:
            query += f" AND timestamp >= '{start_ts}'"
        if end_ts:
            query += f" AND timestamp <= '{end_ts}'"
        if level:
            query += f" AND level = '{level.upper()}'"
        if status_code is not None:
            query += f" AND status_code = {status_code}"
        if keyword:
            safe_keyword = keyword.replace("'", "''")
            query += f" AND (message ILIKE '%{safe_keyword}%' OR metadata ILIKE '%{safe_keyword}%')"

        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        results = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]

        formatted = []
        for row in results:
            item = dict(zip(columns, row))
            try:
                item["metadata"] = json.loads(item.get("metadata", "{}"))
            except Exception:
                item["metadata"] = {}
            formatted.append(item)
        return formatted
    except Exception as e:
        if "No files found that match the pattern" in str(e):
            return []
        logger.error(f"DuckDB search error: {e}")
        return []
    finally:
        if con:
            con.close()


async def get_s3_stats(service_name: str, interval_hours: int = 24) -> dict[str, Any]:
    """Calculate aggregate stats for a service from S3 Parquet logs."""
    if not settings.S3_BUCKET_NAME:
        return {"levels": {}, "series": []}

    con = None
    try:
        con = _get_duckdb_connection()
        s3_path = f"s3://{settings.S3_BUCKET_NAME}/{service_name}/*.parquet"

        level_query = f"""
            SELECT level, COUNT(*) as count
            FROM read_parquet('{s3_path}')
            WHERE timestamp >= (NOW() - INTERVAL '{interval_hours} HOURS')
            GROUP BY level
        """
        level_counts = con.execute(level_query).fetchall()

        series_query = f"""
            SELECT date_trunc('minute', CAST(timestamp AS TIMESTAMP)) as bucket, COUNT(*) as count
            FROM read_parquet('{s3_path}')
            WHERE timestamp >= (NOW() - INTERVAL '{interval_hours} HOURS')
            GROUP BY bucket
            ORDER BY bucket ASC
        """
        time_series = con.execute(series_query).fetchall()

        return {
            "levels": {row[0]: row[1] for row in level_counts},
            "series": [
                {
                    "timestamp": row[0].isoformat()
                    if hasattr(row[0], "isoformat")
                    else str(row[0]),
                    "count": row[1],
                }
                for row in time_series
            ],
        }
    except Exception as e:
        if "No files found that match the pattern" in str(e):
            return {"levels": {}, "series": []}
        logger.error(f"DuckDB stats error: {e}")
        return {"levels": {}, "series": []}
    finally:
        if con:
            con.close()


async def purge_s3_logs(service_name: str, retention_minutes: int) -> int:
    """Delete S3 parquet objects older than retention limit for a service."""
    if not settings.S3_BUCKET_NAME:
        return 0

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.S3_ACCESS_KEY_ID,
        aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
        region_name=settings.S3_REGION_NAME,
    )

    cutoff_dt = datetime.now(timezone.utc) - (
        retention_minutes * datetime.resolution.resolution
        if hasattr(datetime.resolution, "resolution")
        else __import__("datetime").timedelta(minutes=retention_minutes)
    )

    deleted_count = 0
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=settings.S3_BUCKET_NAME, Prefix=f"{service_name}/"
        )
        for page in pages:
            if "Contents" not in page:
                continue
            to_delete = []
            for obj in page["Contents"]:
                if obj["LastModified"] < cutoff_dt:
                    to_delete.append({"Key": obj["Key"]})
            if to_delete:
                s3_client.delete_objects(
                    Bucket=settings.S3_BUCKET_NAME, Delete={"Objects": to_delete}
                )
                deleted_count += len(to_delete)
    except Exception as e:
        logger.error(f"Failed to purge S3 logs for {service_name}: {e}")

    return deleted_count

import os
import logging
import duckdb
import boto3
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.core.config import settings

logger = logging.getLogger(__name__)

async def archive_partition(conn, p_name: str, service_name: str, partition_date_str: str) -> bool:
    if not settings.S3_BUCKET_NAME:
        return False
        
    logger.info(f"Archiving partition {p_name} to S3...")
    
    # Fetch all rows from the partition
    query = f"SELECT timestamp, level, status_code, message, metadata::text FROM {p_name}"
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
        data.append({
            "timestamp": row["timestamp"].isoformat(),
            "level": row["level"],
            "status_code": row["status_code"],
            "message": row["message"] or "",
            "metadata": row["metadata"] or "{}"
        })
        
    local_path = f"/tmp/{p_name}.parquet"
    
    # Write to Parquet using DuckDB
    con = None
    try:
        con = duckdb.connect()
        con.execute(f"COPY (SELECT * FROM data) TO '{local_path}' (FORMAT PARQUET)")
    except Exception as e:
        logger.error(f"Failed to generate parquet file: {e}")
        return False
    finally:
        if con:
            con.close()
        
    # Upload to S3
    s3_key = f"{service_name}/{partition_date_str}.parquet"
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        s3.upload_file(local_path, settings.S3_BUCKET_NAME, s3_key)
        logger.info(f"Successfully archived {p_name} to s3://{settings.S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to S3: {e}")
        return False
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            
    return True

async def search_archive(
    service_name: str, 
    start_ts: str, 
    end_ts: str, 
    level: Optional[str] = None, 
    keyword: Optional[str] = None, 
    limit: int = 100
) -> List[Dict[str, Any]]:
    if not settings.S3_BUCKET_NAME:
        logger.error("S3_BUCKET_NAME not configured")
        return []
        
    con = None
    try:
        con = duckdb.connect()
        # Load AWS extensions
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure AWS credentials for DuckDB
        if settings.AWS_ACCESS_KEY_ID:
            con.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
                    SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
                    REGION '{settings.AWS_REGION}'
                );
            """)
            
        # Build query using wildcard
        # DuckDB automatically uses Parquet min/max statistics to skip scanning irrelevant files!
        s3_path = f"s3://{settings.S3_BUCKET_NAME}/{service_name}/*.parquet"
        
        query = f"SELECT * FROM read_parquet('{s3_path}') WHERE timestamp >= '{start_ts}' AND timestamp <= '{end_ts}'"
        
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
            except:
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

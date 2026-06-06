# Velicor: Simplified Log Ingestion

A lean log ingestion proxy that routes service logs directly to PostgreSQL.

## Features
- **Dynamic Service Isolation**: Automatically creates a separate table for each unique `service_name`.
- **Async Batching**: Buffers logs in memory and performs bulk inserts to Postgres for maximum throughput.
- **Live Tail**: WebSocket support for real-time log monitoring.
- **Extreme Simplicity**: No file-system indexing or complex retrieval logic—just high-speed ingestion.

## Setup
1. Configure your database:
   ```bash
   export LOG_INGEST_POSTGRES_URL="postgresql://user:password@host:5432/db"
   ```
2. Run Velicor:
   ```bash
   uvicorn app.main:app --port 9000
   ```

## API
### Ingest
`POST /api/v1/ingest`
Accepts a JSON object or a list of objects.
Example:
```json
{
  "service_name": "service-name",
  "level": "INFO",
  "message": "User logged in",
  "metadata": {"user_id": 123}
}
```

### Live Tail
`WS /api/v1/live`
Streams all incoming logs.

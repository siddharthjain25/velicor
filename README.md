# 🚀 Velicor: High-Performance Log Ingestion & Analytics

Velicor is a modern, lightweight log ingestion proxy designed for microservice environments. It bridges the gap between raw service logs and actionable insights by routing telemetry directly to PostgreSQL with real-time broadcasting, proactive alerting, and built-in analytics.

---

## ✨ Key Features

*   **⚡ High-Speed Ingestion**: Optimized for tens of thousands of logs per second using async batching and connection pooling.
*   **📊 Real-time Analytics**: Instant visibility into log levels, error rates, and activity "heartbeats" via a responsive dashboard.
*   **🚨 Proactive Alerting**: Configurable Webhooks for Slack, Discord, or custom endpoints. Get notified the second a `FATAL` error occurs.
*   **🛡️ Dynamic Isolation**: Each service gets its own PostgreSQL table with automatic schema management and GIN/FTS indexing.
*   **☁️ Serverless Aware**: Special optimizations for Vercel/AWS Lambda to prevent background task freezing and database connection exhaustion.
*   **📱 Fully Responsive**: A modern React-based UI that works perfectly on desktop and mobile.

---

## 🛠️ Tech Stack

-   **Backend**: FastAPI (Python 3.12+), AsyncPG, Motor (MongoDB Driver).
-   **Database**: 
    -   **PostgreSQL**: High-speed log storage with Full-Text Search.
    -   **MongoDB**: Metadata management for services, users, and webhooks.
-   **Frontend**: React 19, TypeScript, Tailwind CSS 4, Lucide React.
-   **Real-time**: WebSockets (Persistent) & Fast Polling (Serverless Fallback).

---

## 📐 Architecture & Flow

### Log Ingestion Flow
```mermaid
graph TD
    A[Microservice] -->|JSON POST| B(Velicor Ingest API)
    B --> C{Platform Type}
    C -->|Persistent Server| D[Async Queue]
    C -->|Serverless/Vercel| E[Synchronous Flush]
    
    D --> F[Batch Flush - 2s/5000 logs]
    E --> G[Immediate Postgres Write]
    F --> G
    
    B --> H[WebSocket Broadcast]
    B --> I[Webhook Alert Check]
    
    G --> J[(PostgreSQL Logs)]
    H --> K[Live UI Terminal]
    I -->|Match| L[Slack/Discord Alert]
```

### Analytics & Search Flow
```mermaid
graph LR
    A[Velicor UI] -->|Search/Stats Request| B[Velicor API]
    B --> C[(PostgreSQL)]
    C -->|GIN Index| D[Metadata Filter]
    C -->|FTS tsvector| E[Keyword Search]
    C -->|Aggregate| F[Traffic Trends]
    D & E & F --> B
    B --> A
```

---

## 🚀 Getting Started

### 1. Environment Configuration
Create a `.env` file in the root directory:
```bash
# Databases
LOG_INGEST_POSTGRES_URL=postgresql://user:pass@host:6543/db
LOG_INGEST_MONGO_URI=mongodb://user:pass@host:27017/db

# Security
LOG_INGEST_JWT_SECRET_KEY=your_random_secret
LOG_INGEST_SERVERLESS_MODE=True # If on Vercel
```

### 2. Backend Setup
```bash
cd velicor
pip install -r requirements.txt
uvicorn index:app --port 9000
```

### 3. Frontend Setup
```bash
cd velicor-frontend
npm install
npm run dev
```

---

## 📡 SDK & Integration

### Python
```python
import requests
import time

def log_to_velicor(level, message, metadata=None):
    payload = {
        "level": level, "message": message, "timestamp": time.time(),
        "service_name": "my-service", "metadata": metadata or {}
    }
    requests.post("https://your-velicor.com/api/v1/ingest", 
                  json=payload, headers={"x-api-key": "YOUR_KEY"})
```

### Node.js
```javascript
const axios = require('axios');

const logToVelicor = async (level, message, metadata = {}) => {
  await axios.post('https://your-velicor.com/api/v1/ingest', {
    level, message, timestamp: new Date().toISOString(),
    service_name: 'my-service', ...metadata
  }, { headers: { 'x-api-key': 'YOUR_KEY' } });
};
```

---

## 🔒 Security

-   **Service Isolation**: API Keys are scoped to specific services. One compromised key cannot access logs from another service.
-   **Row Level Security (RLS)**: Automatically enabled on all PostgreSQL tables to prevent unauthorized exposure via public database APIs.
-   **JWT Auth**: Industry-standard authentication for the dashboard and administrative actions.

---

## 📈 Performance Optimizations

-   **Connection Reuse**: Warm instances on Vercel maintain database connections to avoid handshake overhead.
-   **Index Strategy**: 
    -   `B-Tree` for timestamps and status codes.
    -   `GIN` for structured JSONB metadata.
    -   `Full-Text Search` for high-speed keyword matching in log messages.
-   **Memory Caching**: 5-minute TTL cache for API key validation to reduce MongoDB load.

---
Velicor Systems © 2026. High-density telemetry simplified.

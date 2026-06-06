import time
import requests
import json
from datetime import datetime, timezone

BASE_URL = "http://localhost:9000"

def test_single_ingest():
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_name": "test-service",
        "level": "INFO",
        "message": "Hello, this is a test log message",
        "metadata": {"key": "value"}
    }
    response = requests.post(f"{BASE_URL}/api/v1/ingest", json=payload)
    print(f"Single ingest status: {response.status_code}")
    print(f"Response: {response.json()}")

def test_batch_ingest():
    payload = [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service_name": "test-service",
            "level": "WARN",
            "message": f"Test message {i}",
        } for i in range(10)
    ]
    response = requests.post(f"{BASE_URL}/api/v1/ingest", json=payload)
    print(f"Batch ingest status: {response.status_code}")
    print(f"Response: {response.json()}")

def check_health():
    response = requests.get(f"{BASE_URL}/health")
    print(f"Health check: {response.json()}")

def test_search():
    # Search for the logs we just ingested
    now_ts = int(time.time())
    start_ts = now_ts - 3600
    end_ts = now_ts + 3600
    
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "service": "test-service",
        "keyword": "Hello"
    }
    response = requests.get(f"{BASE_URL}/api/v1/search", params=params)
    print(f"Search status: {response.status_code}")
    print(f"Search results: {response.json()}")

if __name__ == "__main__":
    try:
        test_single_ingest()
        test_batch_ingest()
        check_health()
        print("\nWaiting for flush and index (3s)...")
        time.sleep(3)
        check_health()
        test_search()
    except Exception as e:
        print(f"Error connecting to server: {e}")

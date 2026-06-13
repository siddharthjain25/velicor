import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone


def test_health(client):
    """Verifies the health check endpoint returns 200 OK."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_ingest_missing_api_key(client):
    """Verifies that an ingestion request without an API key fails with 401."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "message": "Hello, this is a test log message",
    }
    response = client.post("/api/v1/ingest", json=payload)
    assert response.status_code == 401
    assert "Missing API Key" in response.json()["message"]


@patch("app.api.v1.endpoints.get_service_from_key")
def test_ingest_invalid_api_key(mock_get_service, client):
    """Verifies that an ingestion request with an invalid API key fails with 403."""
    mock_get_service.return_value = None
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "message": "Hello, this is a test log message",
    }
    response = client.post(
        "/api/v1/ingest", json=payload, headers={"X-API-Key": "invalid-key"}
    )
    assert response.status_code == 403
    assert "Invalid API Key" in response.json()["message"]


@patch("app.api.v1.endpoints.get_service_from_key")
def test_single_ingest_success(mock_get_service, client):
    """Verifies that a valid single log ingestion request succeeds with 200."""
    mock_get_service.return_value = {
        "name": "test-service",
        "user_id": "60c72b2f9b1d8e1f88a8f12a",
    }
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "message": "Hello, this is a test log message",
    }

    response = client.post(
        "/api/v1/ingest", json=payload, headers={"X-API-Key": "valid-key"}
    )
    assert response.status_code == 200
    assert response.json()["status"] in ["accepted", "created"]
    assert response.json()["processed"] == 1


@patch("app.api.v1.endpoints.get_service_from_key")
@patch("app.db.postgres.pg_manager.search")
def test_search_logs(mock_search, mock_get_service, client):
    """Verifies that log search works correctly and returns mocked search results."""
    mock_get_service.return_value = {
        "name": "test-service",
        "user_id": "60c72b2f9b1d8e1f88a8f12a",
    }
    mock_search.return_value = [
        {"timestamp": "2026-06-13T09:00:00Z", "level": "INFO", "message": "Log 1"},
        {"timestamp": "2026-06-13T09:01:00Z", "level": "INFO", "message": "Log 2"},
    ]

    response = client.get(
        "/api/v1/search",
        params={"service": "test-service", "keyword": "Log"},
        headers={"X-API-Key": "valid-key"},
    )
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 2
    assert results[0]["message"] == "Log 1"
    assert results[1]["message"] == "Log 2"

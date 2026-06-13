import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from app.core.config import settings

@pytest.fixture(autouse=True)
def mock_db_managers():
    """Mock connection managers and db methods to prevent real connection attempts during tests."""
    # Force serverless mode to disable background worker tasks (avoiding async hangs)
    settings.SERVERLESS_MODE = True
    
    with patch("app.db.postgres.pg_manager.connect", new_callable=AsyncMock), \
         patch("app.db.postgres.pg_manager.disconnect", new_callable=AsyncMock), \
         patch("app.db.postgres.pg_manager.insert_batch", new_callable=AsyncMock), \
         patch("app.db.postgres.pg_manager.search", new_callable=AsyncMock), \
         patch("app.db.mongo.mongo_manager.connect", new_callable=AsyncMock), \
         patch("app.db.mongo.mongo_manager.disconnect", new_callable=AsyncMock), \
         patch("app.db.redis.redis_manager.connect", new_callable=AsyncMock), \
         patch("app.db.redis.redis_manager.disconnect", new_callable=AsyncMock):
        yield

@pytest.fixture
def client():
    """Provides a TestClient initialized with the lifespan context."""
    from app.main import app
    with TestClient(app) as c:
        yield c

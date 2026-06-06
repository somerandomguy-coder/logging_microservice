import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database import get_collection, connect_to_mongo, close_mongo_connection
from app.services.log_service import queue_manager
from app.config import settings

# Override config for testing
settings.mongo_db_name = "test_log_db"
settings.redis_url = "redis://localhost:6379/1"  # Use Redis Database 1 for isolation
settings.redis_queue_name = "test_log_ingestion_queue"
settings.batch_size = 5          # Small batch size for faster testing
settings.batch_interval = 0.5    # Small batch interval for faster tests
settings.api_keys = ""           # Keep auth open by default for standard tests


@pytest_asyncio.fixture(autouse=True)
async def setup_test_env():
    # Connect to MongoDB and start batch queue manager
    await connect_to_mongo()
    await queue_manager.start()
    
    # Clean the test collection
    collection = get_collection()
    if collection is not None:
        await collection.delete_many({})
        
    # Clean the test Redis queue
    if queue_manager.redis is not None:
        await queue_manager.redis.delete(settings.redis_queue_name)
        
    yield
    
    # Gracefully stop queue and close MongoDB connection
    await queue_manager.stop()
    await close_mongo_connection()

@pytest.mark.asyncio
async def test_health_check(setup_test_env):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

@pytest.mark.asyncio
async def test_ingest_single_log(setup_test_env):
    payload = {
        "service_name": "test-service",
        "level": "INFO",
        "message": "Hello test log",
        "metadata": {"user_id": 123}
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/v1/logs", json=payload)
    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}

    # Wait for the batch worker to flush it from Redis to MongoDB
    await asyncio.sleep(0.7)

    collection = get_collection()
    doc = await collection.find_one({"service_name": "test-service"})
    assert doc is not None
    assert doc["message"] == "Hello test log"
    assert doc["metadata"]["user_id"] == 123

@pytest.mark.asyncio
async def test_ingest_multiple_logs_batching(setup_test_env):
    payloads = [
        {
            "service_name": f"test-service-{i}",
            "level": "ERROR" if i % 2 == 0 else "INFO",
            "message": f"Log item {i}",
            "metadata": {"index": i}
        } for i in range(10)
    ]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Ingest bulk
        response = await ac.post("/api/v1/logs", json=payloads)
    assert response.status_code == 202

    # Wait for batch worker to flush
    await asyncio.sleep(0.7)

    collection = get_collection()
    count = await collection.count_documents({})
    assert count == 10

@pytest.mark.asyncio
async def test_query_and_stats(setup_test_env):
    # Ingest some test logs
    payloads = [
        {"service_name": "auth-service", "level": "INFO", "message": "User login success", "metadata": {"ip": "1.1.1.1"}},
        {"service_name": "auth-service", "level": "WARNING", "message": "Failed login attempt", "metadata": {"ip": "1.1.1.1"}},
        {"service_name": "payment-service", "level": "ERROR", "message": "Charge failed", "metadata": {"amount": 50}},
    ]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/api/v1/logs", json=payloads)
        # Wait for flush
        await asyncio.sleep(0.7)

        # 1. Test Query Service Name
        resp = await ac.get("/api/v1/logs?service_name=auth-service")
        assert resp.status_code == 200
        logs = resp.json()
        assert len(logs) == 2

        # 2. Test Query level
        resp = await ac.get("/api/v1/logs?level=ERROR")
        assert resp.status_code == 200
        logs = resp.json()
        assert len(logs) == 1
        assert logs[0]["service_name"] == "payment-service"

        # 3. Test Keyword search (MongoDB Text Index Query)
        resp = await ac.get("/api/v1/logs?message=login")
        assert resp.status_code == 200
        logs = resp.json()
        assert len(logs) == 2

        # 4. Test Stats
        resp = await ac.get("/api/v1/logs/stats")
        assert resp.status_code == 200
        stats = resp.json()
        assert stats["total_logs"] == 3
        assert stats["by_service"]["auth-service"] == 2
        assert stats["by_level"]["INFO"] == 1
        assert len(stats["recent_errors"]) == 1

        # 5. Test Queue length stats
        resp = await ac.get("/api/v1/logs/stats/queue")
        assert resp.status_code == 200
        q_stats = resp.json()
        assert q_stats["queue_length"] == 0
        assert q_stats["healthy"] is True

@pytest.mark.asyncio
async def test_api_key_auth(setup_test_env):
    # Enable API Keys validation
    settings.api_keys = "auth-service-key,payment-key"
    
    payload = {
        "service_name": "test-service",
        "level": "INFO",
        "message": "Auth test log"
    }
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. Ingest request without X-API-Key -> should fail (401)
        resp = await ac.post("/api/v1/logs", json=payload)
        assert resp.status_code == 401
        
        # 2. Ingest request with wrong X-API-Key -> should fail (401)
        resp = await ac.post("/api/v1/logs", json=payload, headers={"X-API-Key": "invalid-secret"})
        assert resp.status_code == 401
        
        # 3. Ingest request with valid X-API-Key -> should succeed (202)
        resp = await ac.post("/api/v1/logs", json=payload, headers={"X-API-Key": "payment-key"})
        assert resp.status_code == 202
        
        # 4. Query stats request without X-API-Key -> should fail (401)
        resp = await ac.get("/api/v1/logs/stats")
        assert resp.status_code == 401
        
        # 5. Query stats request with valid X-API-Key -> should succeed (200)
        resp = await ac.get("/api/v1/logs/stats", headers={"X-API-Key": "auth-service-key"})
        assert resp.status_code == 200
        
    # Reset API key validation to empty
    settings.api_keys = ""

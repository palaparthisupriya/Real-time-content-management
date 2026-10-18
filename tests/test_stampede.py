import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from src.main import app, redis_cache, l1_cache
from src.services.metrics import metrics
from src.services.oracle import oracle_service


@pytest_asyncio.fixture
async def async_client():
    metrics.reset()
    oracle_service.reset()
    l1_cache.clear()
    await redis_cache.connect()
    await redis_cache.flush_all()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    await redis_cache.flush_all()
    await redis_cache.disconnect()


@pytest.mark.asyncio
async def test_concurrent_cache_stampede(async_client: AsyncClient):
    """
    Validates cache stampede protection under high concurrent load:
    When 10 concurrent requests target the same missing key:
    - Only 1 request acquires the lock and calls the slow Oracle.
    - 9 concurrent requests wait for the lock holder to populate the cache.
    - Resulting in:
        oracle_service.invocation_count == 1
        cache_misses == 1
        stampedes_prevented == 9
        all 10 requests successfully receive data.
    """
    key = "test_key_stampede_1"

    # Fire 10 concurrent requests simultaneously
    tasks = [async_client.get(f"/data/{key}") for _ in range(10)]
    responses = await asyncio.gather(*tasks)

    # Verify all responses succeeded and have identical payload
    expected_data = f"Simulated_Data_Payload_For_{key}"
    for resp in responses:
        assert resp.status_code == 200
        json_data = resp.json()
        assert json_data["id"] == key
        assert json_data["data"] == expected_data

    # Verify Oracle was called exactly once
    assert oracle_service.invocation_count == 1

    # Verify metrics
    metrics_res = await async_client.get("/metrics")
    assert metrics_res.status_code == 200
    m = metrics_res.json()
    assert m["cache_misses"] == 1
    assert m["stampedes_prevented"] == 9

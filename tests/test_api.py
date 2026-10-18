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
async def test_get_data_endpoint(async_client: AsyncClient):
    # First request: oracle fetch
    res1 = await async_client.get("/data/item_1")
    assert res1.status_code == 200
    data1 = res1.json()
    assert data1["id"] == "item_1"
    assert data1["key"] == "item_1"
    assert data1["data"] == "Simulated_Data_Payload_For_item_1"

    # Second request: L1 hit
    res2 = await async_client.get("/data/item_1")
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["data"] == "Simulated_Data_Payload_For_item_1"

    # Verify metrics
    metrics_res = await async_client.get("/metrics")
    assert metrics_res.status_code == 200
    m = metrics_res.json()
    assert m["l1_hits"] == 1
    assert m["cache_misses"] == 1


@pytest.mark.asyncio
async def test_get_data_bypass_cache(async_client: AsyncClient):
    # Query normal
    await async_client.get("/data/item_bypass")
    assert metrics.get_metrics()["cache_misses"] == 1

    # Query with bypass_cache=true
    res = await async_client.get("/data/item_bypass?bypass_cache=true")
    assert res.status_code == 200
    data = res.json()
    assert data["data"] == "Simulated_Data_Payload_For_item_bypass"
    assert metrics.get_metrics()["cache_misses"] == 2


@pytest.mark.asyncio
async def test_metrics_schema_exact_keys(async_client: AsyncClient):
    res = await async_client.get("/metrics")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")
    data = res.json()

    # Must contain exactly the 5 fields specified in the prompt
    expected_keys = {"l1_hits", "l2_hits", "cache_misses", "stampedes_prevented", "l1_evictions"}
    assert set(data.keys()) == expected_keys
    for k in expected_keys:
        assert isinstance(data[k], int)

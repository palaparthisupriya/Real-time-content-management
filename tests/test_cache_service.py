import pytest
import pytest_asyncio
from src.cache.lru_cache import LRUCache
from src.cache.redis_cache import RedisCache
from src.services.cache_service import CacheService
from src.services.metrics import metrics
from src.services.oracle import OracleService


@pytest_asyncio.fixture
async def redis_client():
    client = RedisCache("redis://localhost:6379/0")
    await client.connect()
    await client.flush_all()
    yield client
    await client.flush_all()
    await client.disconnect()


@pytest_asyncio.fixture
async def test_setup(redis_client):
    metrics.reset()
    l1 = LRUCache(capacity=5)
    oracle = OracleService(delay_seconds=0.05)  # Fast delay for unit test
    service = CacheService(
        l1_cache=l1,
        l2_cache=redis_client,
        oracle=oracle,
        l2_ttl_seconds=30,
        lock_timeout_ms=3000,
        lock_retry_interval_ms=20,
        lock_max_retries=20,
    )
    return service, l1, redis_client, oracle


@pytest.mark.asyncio
async def test_sequential_read_path_full_flow(test_setup):
    service, l1, l2, oracle = test_setup
    key = "user:123"

    # Step 1: Initial query -> L1 & L2 miss -> Oracle fetch
    val1 = await service.get_data(key)
    assert val1 == f"Simulated_Data_Payload_For_{key}"
    assert oracle.invocation_count == 1
    m = metrics.get_metrics()
    assert m["cache_misses"] == 1
    assert m["l1_hits"] == 0
    assert m["l2_hits"] == 0

    # Verify both L1 and L2 are populated
    assert l1.get(key) == val1
    assert await l2.get(key) == val1

    # Step 2: Query again -> L1 hit
    val2 = await service.get_data(key)
    assert val2 == val1
    assert oracle.invocation_count == 1
    m = metrics.get_metrics()
    assert m["l1_hits"] == 1
    assert m["l2_hits"] == 0
    assert m["cache_misses"] == 1

    # Step 3: Evict from L1 manually to simulate server restart / L1 miss
    l1.clear()
    assert l1.get(key) is None

    # Step 4: Query again -> L1 miss, L2 hit -> promoted to L1
    val3 = await service.get_data(key)
    assert val3 == val1
    assert oracle.invocation_count == 1
    m = metrics.get_metrics()
    assert m["l1_hits"] == 1
    assert m["l2_hits"] == 1
    assert m["cache_misses"] == 1

    # Verify key was promoted back to L1
    assert l1.get(key) == val1

    # Step 5: Query again -> now served directly from L1 again
    val4 = await service.get_data(key)
    assert val4 == val1
    m = metrics.get_metrics()
    assert m["l1_hits"] == 2
    assert m["l2_hits"] == 1


@pytest.mark.asyncio
async def test_bypass_cache(test_setup):
    service, l1, l2, oracle = test_setup
    key = "user:bypass"

    # Populate cache first
    await service.get_data(key)
    assert oracle.invocation_count == 1
    assert metrics.get_metrics()["cache_misses"] == 1

    # Normal fetch would hit L1
    await service.get_data(key)
    assert metrics.get_metrics()["l1_hits"] == 1

    # Now bypass cache
    val = await service.get_data(key, bypass_cache=True)
    assert val == f"Simulated_Data_Payload_For_{key}"
    # Oracle was hit again
    assert oracle.invocation_count == 2
    assert metrics.get_metrics()["cache_misses"] == 2
    # L1 hits did not increase
    assert metrics.get_metrics()["l1_hits"] == 1

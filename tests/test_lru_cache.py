import pytest
from src.cache.lru_cache import LRUCache, Node
from src.services.metrics import metrics


def test_node_initialization():
    node = Node("key1", "val1")
    assert node.key == "key1"
    assert node.value == "val1"
    assert node.prev is None
    assert node.next is None


def test_lru_cache_basic_get_put():
    metrics.reset()
    cache = LRUCache(capacity=2)

    assert cache.get("a") is None
    cache.put("a", "1")
    assert cache.get("a") == "1"
    assert len(cache) == 1


def test_lru_cache_update_existing_key():
    metrics.reset()
    cache = LRUCache(capacity=2)
    cache.put("a", "1")
    cache.put("a", "updated_1")

    assert cache.get("a") == "updated_1"
    assert len(cache) == 1
    assert metrics.get_metrics()["l1_evictions"] == 0


def test_lru_cache_eviction_policy():
    metrics.reset()
    cache = LRUCache(capacity=3)

    # Insert A, B, C
    cache.put("A", "val_A")
    cache.put("B", "val_B")
    cache.put("C", "val_C")
    assert len(cache) == 3
    assert metrics.get_metrics()["l1_evictions"] == 0

    # Access A to make it most recently used (Order: A, C, B -> B is LRU)
    assert cache.get("A") == "val_A"

    # Insert D -> should evict B (the least recently used)
    cache.put("D", "val_D")
    assert len(cache) == 3
    assert metrics.get_metrics()["l1_evictions"] == 1

    # B should be gone
    assert cache.get("B") is None
    # A, C, D should remain
    assert cache.get("A") == "val_A"
    assert cache.get("C") == "val_C"
    assert cache.get("D") == "val_D"


def test_lru_cache_multiple_evictions_and_metrics():
    metrics.reset()
    cache = LRUCache(capacity=2)

    cache.put("1", "one")
    cache.put("2", "two")
    cache.put("3", "three")  # evicts 1
    cache.put("4", "four")   # evicts 2

    assert len(cache) == 2
    assert cache.get("1") is None
    assert cache.get("2") is None
    assert cache.get("3") == "three"
    assert cache.get("4") == "four"

    assert metrics.get_metrics()["l1_evictions"] == 2


def test_lru_cache_clear():
    metrics.reset()
    cache = LRUCache(capacity=2)
    cache.put("x", "10")
    cache.put("y", "20")
    assert len(cache) == 2

    cache.clear()
    assert len(cache) == 0
    assert cache.get("x") is None
    assert cache.get("y") is None


def test_lru_cache_invalid_capacity():
    with pytest.raises(ValueError):
        LRUCache(capacity=0)
    with pytest.raises(ValueError):
        LRUCache(capacity=-5)

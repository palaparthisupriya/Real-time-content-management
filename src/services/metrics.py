import threading
from typing import Dict


class MetricsManager:
    """
    Thread-safe metrics manager tracking caching performance metrics.
    Tracks:
    - l1_hits: Count of successful data retrievals from the in-memory LRU.
    - l2_hits: Count of successful data retrievals from Redis.
    - cache_misses: Count of times data was found in neither cache, requiring an Oracle fetch.
    - stampedes_prevented: Count of concurrent requests that waited for lock and resolved from cache.
    - l1_evictions: Count of items removed from L1 cache due to capacity limits.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._l1_hits: int = 0
        self._l2_hits: int = 0
        self._cache_misses: int = 0
        self._stampedes_prevented: int = 0
        self._l1_evictions: int = 0

    def increment_l1_hits(self, count: int = 1) -> None:
        with self._lock:
            self._l1_hits += count

    def increment_l2_hits(self, count: int = 1) -> None:
        with self._lock:
            self._l2_hits += count

    def increment_cache_misses(self, count: int = 1) -> None:
        with self._lock:
            self._cache_misses += count

    def increment_stampedes_prevented(self, count: int = 1) -> None:
        with self._lock:
            self._stampedes_prevented += count

    def increment_l1_evictions(self, count: int = 1) -> None:
        with self._lock:
            self._l1_evictions += count

    def get_metrics(self) -> Dict[str, int]:
        with self._lock:
            return {
                "l1_hits": self._l1_hits,
                "l2_hits": self._l2_hits,
                "cache_misses": self._cache_misses,
                "stampedes_prevented": self._stampedes_prevented,
                "l1_evictions": self._l1_evictions,
            }

    def reset(self) -> None:
        with self._lock:
            self._l1_hits = 0
            self._l2_hits = 0
            self._cache_misses = 0
            self._stampedes_prevented = 0
            self._l1_evictions = 0


# Global metrics singleton
metrics = MetricsManager()

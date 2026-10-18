import asyncio
import logging
import uuid
from typing import Any, Optional
from src.cache.lru_cache import LRUCache
from src.cache.redis_cache import RedisCache
from src.config.config import settings
from src.services.metrics import metrics
from src.services.oracle import OracleService, oracle_service

logger = logging.getLogger(__name__)


class CacheService:
    """
    Orchestrates the multi-layer caching system:
    - Layer 1: In-Memory LRU Cache
    - Layer 2: Distributed Redis Cache
    - Distributed Locking for Cache Stampede Protection
    - Fallback: Simulated Oracle Data Source
    """

    def __init__(
        self,
        l1_cache: LRUCache,
        l2_cache: RedisCache,
        oracle: OracleService,
        l2_ttl_seconds: int = settings.L2_TTL_SECONDS,
        lock_timeout_ms: int = settings.LOCK_TIMEOUT_MS,
        lock_retry_interval_ms: int = settings.LOCK_RETRY_INTERVAL_MS,
        lock_max_retries: int = settings.LOCK_MAX_RETRIES,
    ) -> None:
        self.l1_cache = l1_cache
        self.l2_cache = l2_cache
        self.oracle = oracle
        self.l2_ttl_seconds = l2_ttl_seconds
        self.lock_timeout_ms = lock_timeout_ms
        self.lock_retry_interval_ms = lock_retry_interval_ms
        self.lock_max_retries = lock_max_retries

    async def get_data(self, key: str, bypass_cache: bool = False) -> str:
        """
        Retrieves data for key following the strict multi-layer cache sequence.
        If bypass_cache is True, bypasses both L1 and L2 and the lock, fetches directly
        from Oracle, writes through to L2 and L1, and returns.
        """
        # Bypass cache check
        if bypass_cache:
            logger.info("bypass_cache=True for key '%s': skipping caches", key)
            data = await self.oracle.fetch(key)
            await self._write_through(key, data)
            return data

        # 1. Check L1 In-Memory LRU Cache
        l1_value = self.l1_cache.get(key)
        if l1_value is not None:
            logger.debug("L1 Cache Hit for key: %s", key)
            metrics.increment_l1_hits()
            return str(l1_value)

        # 2. Check L2 Distributed Redis Cache
        l2_value = await self.l2_cache.get(key)
        if l2_value is not None:
            logger.debug("L2 Cache Hit for key: %s", key)
            metrics.increment_l2_hits()
            # Promote to L1
            self.l1_cache.put(key, l2_value)
            return l2_value

        # 3. L1 and L2 Miss: Invoke Cache Stampede Protection (Distributed Lock)
        logger.info("L1 & L2 Cache Miss for key: %s. Initiating stampede protection.", key)
        return await self._fetch_with_stampede_protection(key)

    async def _fetch_with_stampede_protection(self, key: str) -> str:
        """
        Acquires a distributed lock in Redis to fetch from the slow Oracle.
        If the lock cannot be acquired, enters a wait-and-retry loop to read
        the cached data once the lock holder populates it.
        """
        lock_token = uuid.uuid4().hex

        # Attempt to acquire lock
        acquired = await self.l2_cache.acquire_lock(
            key,
            token=lock_token,
            timeout_ms=self.lock_timeout_ms,
        )

        if acquired:
            logger.info("Acquired distributed lock for key: %s", key)
            try:
                # Fetch from slow Oracle
                data = await self.oracle.fetch(key)
                # Write to L2 and L1
                await self._write_through(key, data)
                return data
            finally:
                # Release lock
                await self.l2_cache.release_lock(key, token=lock_token)
                logger.debug("Released distributed lock for key: %s", key)

        # Lock failed: another concurrent request holds the lock.
        # Wait-and-retry loop
        logger.info("Lock held by another request for key: %s. Entering wait-and-retry loop.", key)
        retry_sleep_seconds = self.lock_retry_interval_ms / 1000.0

        for attempt in range(1, self.lock_max_retries + 1):
            await asyncio.sleep(retry_sleep_seconds)

            # Re-check L1
            l1_val = self.l1_cache.get(key)
            if l1_val is not None:
                logger.info(
                    "Resolved from L1 after wait (attempt %d) for key: %s. Stampede prevented.",
                    attempt,
                    key,
                )
                metrics.increment_stampedes_prevented()
                return str(l1_val)

            # Re-check L2
            l2_val = await self.l2_cache.get(key)
            if l2_val is not None:
                logger.info(
                    "Resolved from L2 after wait (attempt %d) for key: %s. Stampede prevented.",
                    attempt,
                    key,
                )
                metrics.increment_stampedes_prevented()
                # Promote to L1
                self.l1_cache.put(key, l2_val)
                return l2_val

            # Try to acquire the lock again in case previous holder finished or timed out
            retry_token = uuid.uuid4().hex
            retry_acquired = await self.l2_cache.acquire_lock(
                key,
                token=retry_token,
                timeout_ms=self.lock_timeout_ms,
            )
            if retry_acquired:
                logger.info("Acquired lock on retry attempt %d for key: %s", attempt, key)
                try:
                    data = await self.oracle.fetch(key)
                    await self._write_through(key, data)
                    return data
                finally:
                    await self.l2_cache.release_lock(key, token=retry_token)

        # Fallback if wait loop exhausted
        logger.warning("Wait-and-retry loop exhausted for key: %s. Fetching from Oracle as fallback.", key)
        data = await self.oracle.fetch(key)
        await self._write_through(key, data)
        return data

    async def _write_through(self, key: str, data: str) -> None:
        """
        Sequential Write Path:
        1. Write to L2 Cache (Redis) with 30s TTL.
        2. Write to L1 Cache (In-Memory LRU).
        """
        await self.l2_cache.put(key, data, ttl_seconds=self.l2_ttl_seconds)
        self.l1_cache.put(key, data)

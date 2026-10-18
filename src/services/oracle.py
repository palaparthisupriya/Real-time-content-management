import asyncio
import logging
from src.config.config import settings
from src.services.metrics import metrics

logger = logging.getLogger(__name__)


class OracleService:
    """
    Simulated Oracle representing a slow primary database or sluggish external API.
    Artificially delays execution by exactly 2.0 seconds and returns deterministic data.
    Tracks all invocations and updates cache_misses metric.
    """

    def __init__(self, delay_seconds: float = 2.0) -> None:
        self.delay_seconds = delay_seconds
        self.invocation_count = 0

    async def fetch(self, key: str) -> str:
        """
        Simulate fetching data from a slow database.
        Increments internal invocation counter and cache_misses metric.
        Delays execution by delay_seconds (2.0s).
        """
        logger.info("Oracle fetching data for key: %s (simulated %ss delay)", key, self.delay_seconds)
        self.invocation_count += 1
        metrics.increment_cache_misses()

        # Simulate slow database query or network latency
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)

        payload = f"Simulated_Data_Payload_For_{key}"
        logger.info("Oracle completed fetch for key: %s", key)
        return payload

    def reset(self) -> None:
        self.invocation_count = 0


# Global oracle singleton initialized with settings delay
oracle_service = OracleService(delay_seconds=settings.ORACLE_DELAY_SECONDS)


async def fetch_from_oracle(key: str) -> str:
    """Convenience helper to fetch from oracle singleton."""
    return await oracle_service.fetch(key)

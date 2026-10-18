import logging
from typing import Optional
import redis.asyncio as aioredis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# Lua script for safe atomic lock release (releases only if value matches token)
RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class RedisCache:
    """
    Asynchronous Redis client wrapper for L2 Cache and Distributed Locking.
    Enforces TTL on all writes and provides SET NX PX distributed locks.
    """

    def __init__(self, redis_url: str) -> None:
        self.redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None
        self._release_script = None

    async def connect(self) -> None:
        """Establishes connection to the Redis server."""
        try:
            self._client = aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._client.ping()
            self._release_script = self._client.register_script(RELEASE_LOCK_SCRIPT)
            logger.info("Successfully connected to Redis at %s", self.redis_url)
        except (RedisError, ConnectionError, OSError) as e:
            logger.error("Failed to connect to Redis at %s: %s", self.redis_url, e)
            self._client = None
            raise

    async def disconnect(self) -> None:
        """Closes the Redis connection pool."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("Closed Redis connection")

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("Redis client is not connected")
        return self._client

    async def get(self, key: str) -> Optional[str]:
        """
        Retrieves a cached value from Redis by key.
        Returns None if key does not exist or if Redis errors occur.
        """
        try:
            return await self.client.get(key)
        except RedisError as e:
            logger.error("Redis GET error for key '%s': %s", key, e)
            return None

    async def put(self, key: str, value: str, ttl_seconds: int = 30) -> bool:
        """
        Writes data to Redis with a mandatory Time-To-Live (TTL).
        Default TTL is 30 seconds.
        """
        try:
            return bool(await self.client.set(name=key, value=value, ex=ttl_seconds))
        except RedisError as e:
            logger.error("Redis SET error for key '%s': %s", key, e)
            return False

    async def acquire_lock(self, key: str, token: str, timeout_ms: int = 5000) -> bool:
        """
        Attempts to acquire a distributed lock for key using SET NX PX.
        Returns True if acquired, False otherwise.
        """
        lock_key = f"lock:{key}"
        try:
            # SET lock:{key} {token} NX PX 5000
            result = await self.client.set(
                name=lock_key,
                value=token,
                nx=True,
                px=timeout_ms,
            )
            return bool(result)
        except RedisError as e:
            logger.error("Error acquiring lock for '%s': %s", lock_key, e)
            return False

    async def release_lock(self, key: str, token: str) -> bool:
        """
        Atomically releases the distributed lock if the token matches.
        Falls back to direct DEL if script execution is not available.
        """
        lock_key = f"lock:{key}"
        try:
            if self._release_script is not None:
                res = await self._release_script(keys=[lock_key], args=[token])
                return bool(res)
            else:
                val = await self.client.get(lock_key)
                if val == token:
                    await self.client.delete(lock_key)
                    return True
                return False
        except RedisError as e:
            logger.error("Error releasing lock for '%s': %s", lock_key, e)
            return False

    async def delete(self, key: str) -> bool:
        """Deletes a key from Redis."""
        try:
            return bool(await self.client.delete(key))
        except RedisError as e:
            logger.error("Redis DELETE error for key '%s': %s", key, e)
            return False

    async def flush_all(self) -> None:
        """Flushes the database (useful for testing)."""
        try:
            await self.client.flushdb()
        except RedisError as e:
            logger.error("Redis flushdb error: %s", e)

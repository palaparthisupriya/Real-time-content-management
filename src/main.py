import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.api.routes import router, set_cache_service
from src.cache.lru_cache import LRUCache
from src.cache.redis_cache import RedisCache
from src.config.config import settings
from src.services.cache_service import CacheService
from src.services.oracle import oracle_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("multi_layer_cache")

# Instantiate singletons
l1_cache = LRUCache(capacity=settings.L1_CAPACITY)
redis_cache = RedisCache(redis_url=settings.REDIS_URL)
cache_service = CacheService(
    l1_cache=l1_cache,
    l2_cache=redis_cache,
    oracle=oracle_service,
    l2_ttl_seconds=settings.L2_TTL_SECONDS,
    lock_timeout_ms=settings.LOCK_TIMEOUT_MS,
    lock_retry_interval_ms=settings.LOCK_RETRY_INTERVAL_MS,
    lock_max_retries=settings.LOCK_MAX_RETRIES,
)
set_cache_service(cache_service)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application."""
    logger.info("Initializing multi-layer caching service...")
    try:
        await redis_cache.connect()
        logger.info("Redis L2 Cache connected successfully at %s", settings.REDIS_URL)
    except Exception as e:
        logger.warning("Could not connect to Redis at startup: %s. Requests requiring L2 will fail or retry.", e)

    yield

    logger.info("Shutting down multi-layer caching service...")
    await redis_cache.disconnect()


app = FastAPI(
    title="Multi-Layer Caching Service",
    description="High-performance two-layer caching service with LRU L1, Redis L2, and stampede protection.",
    version="1.0.0",
    lifespan=lifespan,
)

# Include API routes
app.include_router(router)


@app.get("/")
async def root():
    return {
        "service": "Multi-Layer Caching Service",
        "status": "online",
        "endpoints": {
            "data": "/data/{key}",
            "metrics": "/metrics",
            "docs": "/docs",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
    )

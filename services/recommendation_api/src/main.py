import os
import json
import logging
from typing import Optional, List, Set, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis
import psycopg2
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("recommendation_api")

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "recommendation_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "3600"))

# In-memory fallback for tracking viewed items per user during session
IN_MEMORY_VIEWED_ITEMS: Dict[str, Set[str]] = {}


class RecommendationResponse(BaseModel):
    user_id: str
    recommendations: List[str] = Field(..., max_length=5, description="Up to 5 recommended item IDs")


class ServiceClients:
    """Manages Redis and PostgreSQL client lifecycles with mock support for tests."""
    redis_client: Optional[redis.Redis] = None
    db_pool: Optional[SimpleConnectionPool] = None

    @classmethod
    def get_redis(cls) -> Optional[redis.Redis]:
        if cls.redis_client is None:
            try:
                cls.redis_client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True,
                    socket_connect_timeout=3,
                )
                cls.redis_client.ping()
                logger.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)
            except Exception as e:
                logger.warning("Redis connection failed (%s). Cache will be bypassed.", str(e))
                cls.redis_client = None
        return cls.redis_client

    @classmethod
    def get_db_connection(cls):
        if cls.db_pool is None:
            try:
                cls.db_pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                )
                logger.info("Connected to PostgreSQL at %s:%s", POSTGRES_HOST, POSTGRES_PORT)
            except Exception as e:
                logger.error("Failed to connect to PostgreSQL: %s", str(e))
                raise
        return cls.db_pool.getconn()

    @classmethod
    def release_db_connection(cls, conn):
        if cls.db_pool and conn:
            cls.db_pool.putconn(conn)

    @classmethod
    def close_all(cls):
        if cls.redis_client:
            try:
                cls.redis_client.close()
            except Exception:
                pass
            cls.redis_client = None

        if cls.db_pool:
            try:
                cls.db_pool.closeall()
            except Exception:
                pass
            cls.db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Recommendation API...")
    try:
        ServiceClients.get_redis()
    except Exception as e:
        logger.warning("Redis not ready at startup: %s", str(e))
    try:
        conn = ServiceClients.get_db_connection()
        ServiceClients.release_db_connection(conn)
    except Exception as e:
        logger.warning("PostgreSQL not ready at startup: %s", str(e))
    yield
    # Shutdown
    logger.info("Shutting down Recommendation API...")
    ServiceClients.close_all()


app = FastAPI(
    title="Recommendation API",
    description="High-performance recommendation serving layer with Redis Cache-Aside",
    version="1.0.0",
    lifespan=lifespan,
)


def get_user_preferences_cache_aside(user_id: str, redis_client: Optional[redis.Redis], db_conn) -> Optional[Dict[str, Any]]:
    """
    Implements Cache-Aside Pattern:
    1. Check Redis for key `user:{user_id}:prefs`
    2. If Hit: return deserialized JSON preferences
    3. If Miss: Query PostgreSQL users table
    4. If user doesn't exist in DB: return None (trigger 404)
    5. If found in DB: save to Redis with TTL, return preferences
    """
    cache_key = f"user:{user_id}:prefs"

    # Step 1: Check Redis Cache
    if redis_client:
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data is not None:
                logger.info("Cache HIT for user=%s", user_id)
                return json.loads(cached_data)
        except Exception as e:
            logger.warning("Error reading from Redis cache (%s). Falling back to DB.", str(e))

    # Step 2: Cache Miss - Query PostgreSQL
    logger.info("Cache MISS for user=%s. Querying PostgreSQL...", user_id)
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT preferences FROM users WHERE user_id = %s;", (user_id,))
        row = cursor.fetchone()
        if not row:
            logger.warning("User '%s' not found in database.", user_id)
            return None

        raw_prefs = row[0]
        preferences = raw_prefs if isinstance(raw_prefs, dict) else json.loads(raw_prefs or "{}")

    # Step 3: Populate Redis Cache with TTL
    if redis_client:
        try:
            redis_client.setex(cache_key, REDIS_TTL_SECONDS, json.dumps(preferences))
            logger.info("Stored preferences for user=%s in Redis (TTL=%ds)", user_id, REDIS_TTL_SECONDS)
        except Exception as e:
            logger.warning("Failed to store preferences in Redis: %s", str(e))

    return preferences


def get_viewed_items(user_id: str, redis_client: Optional[redis.Redis]) -> Set[str]:
    """Retrieve set of items previously returned to or viewed by this user."""
    viewed = set()

    # In-memory check
    if user_id in IN_MEMORY_VIEWED_ITEMS:
        viewed.update(IN_MEMORY_VIEWED_ITEMS[user_id])

    # Redis set check
    if redis_client:
        try:
            redis_key = f"user:{user_id}:viewed"
            redis_items = redis_client.smembers(redis_key)
            if redis_items:
                viewed.update(redis_items)
        except Exception as e:
            logger.warning("Error reading viewed items from Redis: %s", str(e))

    return viewed


def record_viewed_items(user_id: str, item_ids: List[str], redis_client: Optional[redis.Redis]):
    """Record newly recommended items to prevent repeat recommendations."""
    if not item_ids:
        return

    # Update in-memory set
    if user_id not in IN_MEMORY_VIEWED_ITEMS:
        IN_MEMORY_VIEWED_ITEMS[user_id] = set()
    IN_MEMORY_VIEWED_ITEMS[user_id].update(item_ids)

    # Update Redis set with 24h expiration
    if redis_client:
        try:
            redis_key = f"user:{user_id}:viewed"
            redis_client.sadd(redis_key, *item_ids)
            redis_client.expire(redis_key, 86400)
        except Exception as e:
            logger.warning("Error writing viewed items to Redis: %s", str(e))


def compute_recommendations(
    user_preferences: Dict[str, Any],
    viewed_items: Set[str],
    db_conn,
    limit: int = 5
) -> List[str]:
    """
    Generate personalized recommendations based on preference vector:
    1. Query candidate items from DB.
    2. Rank candidates by matching user category scores.
    3. Filter out items in viewed_items.
    4. Return up to `limit` item IDs.
    """
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT item_id, categories FROM items;")
        rows = cursor.fetchall()

    scored_items = []
    fallback_items = []

    for item_id, categories_data in rows:
        if item_id in viewed_items:
            continue

        # Parse categories
        if isinstance(categories_data, list):
            item_categories = [str(c).strip().lower() for c in categories_data]
        elif isinstance(categories_data, str):
            try:
                parsed = json.loads(categories_data)
                item_categories = [str(c).strip().lower() for c in parsed] if isinstance(parsed, list) else [categories_data.strip().lower()]
            except Exception:
                item_categories = [categories_data.strip().lower()]
        else:
            item_categories = []

        # Calculate relevance score based on preference vector
        score = sum(int(user_preferences.get(cat, 0)) for cat in item_categories)

        if score > 0:
            scored_items.append((score, item_id))
        else:
            fallback_items.append(item_id)

    # Sort scored items descending by score
    scored_items.sort(key=lambda x: x[0], reverse=True)
    recommendations = [item[1] for item in scored_items[:limit]]

    # If fewer than limit, backfill with unviewed items
    if len(recommendations) < limit:
        for item_id in fallback_items:
            if item_id not in recommendations:
                recommendations.append(item_id)
                if len(recommendations) >= limit:
                    break

    return recommendations[:limit]


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy", "service": "recommendation_api"}


@app.get("/api/recommendations/{user_id}", response_model=RecommendationResponse)
async def get_recommendations(user_id: str):
    redis_client = ServiceClients.get_redis()
    db_conn = None

    try:
        db_conn = ServiceClients.get_db_connection()
    except Exception as e:
        logger.error("Database connection failure: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection error",
        )

    try:
        # Cache-Aside pattern
        preferences = get_user_preferences_cache_aside(user_id, redis_client, db_conn)
        if preferences is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User '{user_id}' not found in database",
            )

        # Retrieve viewed items
        viewed = get_viewed_items(user_id, redis_client)

        # Compute recommendations
        recommendations = compute_recommendations(preferences, viewed, db_conn, limit=5)

        # Record newly recommended items to prevent repeating in this session
        record_viewed_items(user_id, recommendations, redis_client)

        return RecommendationResponse(user_id=user_id, recommendations=recommendations)
    finally:
        if db_conn:
            ServiceClients.release_db_connection(db_conn)

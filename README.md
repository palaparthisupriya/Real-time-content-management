# Multi-layer-caching-service

A high-performance, two-layer caching backend service with Redis designed to protect a slow primary data store (Simulated Oracle) from heavy traffic and cache stampedes.

Features an **In-Memory L1 Cache** with a custom Least Recently Used (LRU) eviction algorithm built from scratch (Hash Map + Doubly Linked List), a **Distributed L2 Cache** using Redis with Time-To-Live (TTL) expiration, and **Distributed Mutual Exclusion Locking** (`SET NX PX`) to eliminate Cache Stampedes (Thundering Herd).

---

## Architecture Overview

```
                          Client Request: GET /data/{key}
                                        │
                                        ▼
                             ┌─────────────────────┐
                             │   FastAPI Router    │
                             └──────────┬──────────┘
                                        │
                             [1] Check L1 LRU Cache
                                 (In-Memory O(1))
                                        │
                       ┌────────────────┴────────────────┐
                       │                                 │
                   (L1 Hit)                          (L1 Miss)
                       │                                 │
                       ▼                                 ▼
             Return Data Immediately           [2] Check L2 Cache
             (l1_hits++)                            (Redis)
                                                         │
                                        ┌────────────────┴────────────────┐
                                        │                                 │
                                    (L2 Hit)                          (L2 Miss)
                                        │                                 │
                                        ▼                                 ▼
                                 Promote to L1                [3] Acquire Redis Lock
                                  (l2_hits++)                 SET lock:{key} token NX PX 5000
                                 Return Data                              │
                                                         ┌────────────────┴────────────────┐
                                                         │                                 │
                                                 (Lock Acquired)                   (Lock Held by Other)
                                                         │                                 │
                                                         ▼                                 ▼
                                                [4] Fetch Slow Oracle             [Wait & Retry Loop]
                                                    (2.0s delay,                   Sleep 100ms
                                                     cache_misses++)               Re-check L1 & L2
                                                         │                         Found -> stampedes_prevented++
                                                         ▼                         Not found -> retry lock
                                                [5] Write-Through
                                                    - Redis L2 (TTL 30s)
                                                    - Memory L1 (LRU)
                                                         │
                                                         ▼
                                                [6] Release Lock
                                                    Return Data
```

---

## Core Components

### 1. Custom L1 In-Memory LRU Cache
- **Data Structure**: Hand-crafted Hash Map (`dict`) coupled with a Doubly Linked List (`Node` pointers with dummy `head` and `tail`).
- **Complexity**: Strict **O(1)** time complexity for both `get` and `put` operations.
- **Eviction**: When capacity is reached, automatically evicts the node immediately preceding the dummy tail (least recently accessed) and increments the `l1_evictions` metric.
- **Independence**: Fully independent implementation without third-party or standard library LRU packages (no `functools.lru_cache`, `OrderedDict`, etc.).

### 2. Distributed L2 Redis Cache
- **Storage**: Shared caching layer across multiple service instances.
- **Time-To-Live (TTL)**: Mandatory 30-second TTL on all keys written to Redis (`SET EX 30`).
- **Promotion**: When data is found in L2, it is automatically promoted to L1 to serve future requests with in-memory latency.

### 3. Distributed Locking & Cache Stampede Protection
- **Locking**: Uses Redis atomic conditional execution: `SET lock:{key} {token} NX PX 5000` (5-second safety expiry prevents deadlocks).
- **Mutual Exclusion**: Only one concurrent request is granted permission to query the slow Oracle.
- **Wait-and-Retry Loop**: Blocked requests sleep for 100ms, iteratively re-checking L1 and L2. When the lock holder populates the cache, waiting requests resolve the data directly from cache and increment `stampedes_prevented`.

### 4. Simulated Slow Oracle
- Artificially delays execution by exactly 2.0 seconds (`await asyncio.sleep(2.0)`).
- Produces deterministic data: `Simulated_Data_Payload_For_{key}`.
- Tracks invocations to accurately maintain the `cache_misses` metric.

---

## Directory Structure

```
multi-layer-caching-service/
├── src/
│   ├── __init__.py
│   ├── main.py                  # FastAPI application entry point & lifecycle
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py            # Route handlers (/data/{key}, /metrics)
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── lru_cache.py         # Custom O(1) LRU Cache (Hash Map + Doubly Linked List)
│   │   └── redis_cache.py       # Async Redis L2 client & distributed locking
│   ├── services/
│   │   ├── __init__.py
│   │   ├── cache_service.py     # Multi-layer orchestration & stampede protection
│   │   ├── metrics.py           # Real-time metrics counters
│   │   └── oracle.py            # Simulated slow Oracle database
│   └── config/
│       ├── __init__.py
│       └── config.py            # Environment configuration with pydantic-settings
├── tests/
│   ├── __init__.py
│   ├── test_lru_cache.py        # LRU eviction, O(1) order & metrics tests
│   ├── test_cache_service.py    # Sequential read/write path & bypass tests
│   ├── test_api.py              # API contract & schema validation tests
│   └── test_stampede.py         # Concurrent 10-worker stampede protection test
├── scripts/
│   └── test_concurrent_stampede.py # Live concurrency verification script
├── docker-compose.yml           # App and Redis container orchestration
├── Dockerfile                   # Multi-stage Python 3.12 container
├── requirements.txt             # Project dependencies
├── .env.example                 # Environment variables template
├── .env                         # Local environment settings
└── README.md                    # Project documentation
```

---

## API Endpoints

### 1. Get Data: `GET /data/{key}`
Retrieves data following the sequential multi-layer cache flow.

- **Path Parameter**: `key` (string)
- **Query Parameter**: `bypass_cache=true` (optional boolean). When `true`, completely bypasses L1 and L2 caches and locking, reads directly from Oracle, writes through to L2 and L1, and returns fresh data.

**Sample Request**:
```bash
curl -X GET "http://localhost:8080/data/item_1"
```

**Sample Response (`200 OK`)**:
```json
{
  "id": "item_1",
  "key": "item_1",
  "data": "Simulated_Data_Payload_For_item_1"
}
```

---

### 2. Metrics: `GET /metrics`
Returns real-time performance statistics across all layers.

- **Content-Type**: `application/json`

**Sample Request**:
```bash
curl -X GET "http://localhost:8080/metrics"
```

**Sample Response (`200 OK`)**:
```json
{
  "l1_hits": 142,
  "l2_hits": 56,
  "cache_misses": 12,
  "stampedes_prevented": 89,
  "l1_evictions": 45
}
```

#### Metric Definitions:
| Metric | Description |
|---|---|
| `l1_hits` | Number of successful cache hits resolved directly from the in-memory LRU. |
| `l2_hits` | Number of successful cache hits resolved from Redis. |
| `cache_misses` | Number of requests where data was missing from both caches, requiring an Oracle fetch. |
| `stampedes_prevented` | Number of concurrent requests that waited for a lock-holder and retrieved data populated into cache. |
| `l1_evictions` | Number of items discarded from L1 cache due to capacity limit. |

---

## Running with Docker (Recommended)

### Prerequisites
- Docker and Docker Compose installed.

### Start the Infrastructure
```bash
docker-compose up --build
```
This spins up:
1. `redis`: Official `redis:alpine` running on port 6379 with automatic health check.
2. `app`: The caching service container running on port 8080 with internal networking `cache-net`.

### Stop the Infrastructure
```bash
docker-compose down
```

---

## Running Locally

### 1. Start Redis
Make sure Redis is running on `localhost:6379`:
```bash
redis-server --port 6379
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Application
```bash
uvicorn src.main:app --host 0.0.0.0 --port 8080
```

---

## Running Tests

Run the complete test suite:
```bash
pytest tests/ -v
```

---

## Cache Stampede Live Verification

With the service running on `http://localhost:8080`, execute the concurrency test:

```bash
python scripts/test_concurrent_stampede.py
```
# Real-time-content-management

# Real-Time Content Recommendation Service with Apache Kafka & Microservices

An enterprise-grade, event-driven recommendation backend designed for low-latency, personalized content delivery at scale. Built with **Apache Kafka (KRaft)**, **FastAPI**, **PostgreSQL (JSONB)**, **Redis (Cache-Aside)**, and orchestrated seamlessly via **Docker Compose**.

---

## Architecture Overview

```mermaid
flowchart TD
    Client["Client / User Application"]

    subgraph "Event Ingestion Domain (Port 8000)"
        IngestAPI["Event Ingestion API<br/>(FastAPI Producer)"]
    end

    subgraph "Streaming Backbone"
        KafkaBroker["Apache Kafka Broker<br/>Topic: user_interactions (KRaft)"]
    end

    subgraph "Asynchronous Processing Domain"
        ProfileWorker["Profile Update Service<br/>(Background Consumer)"]
    end

    subgraph "Storage & Cache Tier"
        PostgresDB[("PostgreSQL Database<br/>Users (JSONB) & Items")]
        RedisCache[("Redis Cache<br/>Cache-Aside & Viewed Items")]
    end

    subgraph "Recommendation Domain (Port 8001)"
        RecAPI["Recommendation API<br/>(Serving Layer)"]
    end

    %% Flow 1: Event Tracking
    Client -- "1. POST /api/events/view" --> IngestAPI
    IngestAPI -- "2. Publish event (202 Accepted)" --> KafkaBroker
    KafkaBroker -- "3. Consume event" --> ProfileWorker
    ProfileWorker -- "4. Read item categories & update JSONB" --> PostgresDB

    %% Flow 2: Recommendation Serving
    Client -- "5. GET /api/recommendations/{user_id}" --> RecAPI
    RecAPI -- "6. Check Redis cache key" --> RedisCache
    RecAPI -. "7. Fallback on cache miss" .-> PostgresDB
    RecAPI -- "8. Store in Redis (TTL=3600s)" --> RedisCache
    RecAPI -- "9. Filter viewed items & return top-5" --> Client
```

---

## Core Microservices & Components

| Component | Port | Technology | Role |
| :--- | :--- | :--- | :--- |
| **Event Ingestion API** | `8000` | FastAPI, `kafka-python-ng` | High-throughput HTTP gateway. Validates interaction payloads, publishes events to `user_interactions` Kafka topic, and returns `202 Accepted`. |
| **Kafka Broker** | `9092` | Bitnami Kafka 3.7 (KRaft) | Distributed messaging buffer. Decouples fast HTTP ingestion from database mutation. |
| **Profile Update Service** | N/A | Python Worker, `psycopg2` | Headless consumer listening to `user_interactions`. Recalculates user category preference vectors and persists them to PostgreSQL `JSONB`. |
| **PostgreSQL Database** | `5432` | PostgreSQL 14 (Alpine) | Persistent storage for `users` (with JSONB preference vectors) and `items` (with JSONB categories & features). Automatically seeded via `seed_db.sql`. |
| **Redis Cache** | `6379` | Redis 7 (Alpine) | Sub-millisecond cache for user preferences implementing the **Cache-Aside pattern**, plus session-level viewed item tracking. |
| **Recommendation API** | `8001` | FastAPI, Redis, PostgreSQL | Read-optimized serving layer. Resolves user preferences via Redis, filters session-viewed items, and scores candidate items by category weight. |

---

## Directory Structure

```text
realtime-recommendation-service/
├── .env.example                       # Documented environment configuration template
├── .env                               # Active configuration file
├── docker-compose.yml                 # Multi-container orchestration with healthchecks
├── README.md                          # Architecture and operational documentation
├── run_tests.sh                       # Automated test suite runner (Linux / macOS / Bash)
├── run_tests.ps1                      # Automated test suite runner (Windows PowerShell)
├── scripts/
│   ├── seed_db.sql                    # Initial schema definition and mock seed data
│   └── e2e_test.py                    # End-to-end integration and verification script
└── services/
    ├── event_ingestion_api/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── src/
    │   │   └── main.py                # FastAPI producer with singleton Kafka connection
    │   └── tests/
    │       └── test_main.py           # Unit tests (validation, singleton, error handling)
    ├── profile_update_service/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── src/
    │   │   └── consumer.py            # Kafka consumer with resilient error recovery
    │   └── tests/
    │       └── test_consumer.py       # Unit tests (parsing, category calculation, DB upsert)
    └── recommendation_api/
        ├── Dockerfile
        ├── requirements.txt
        ├── src/
        │   └── main.py                # FastAPI serving layer (Cache-Aside, filtering)
        └── tests/
            └── test_main.py           # Unit tests (Cache hit/miss, scoring, 404 handling)
```

---

## Architectural Patterns Implemented

### 1. Asynchronous Event-Driven Decoupling (Apache Kafka)
Instead of synchronously blocking the client while writing to the database, user interactions are accepted immediately:
- Ingestion API acts purely as a non-blocking producer with retries (`retries=3`).
- Returns HTTP `202 Accepted` in single-digit milliseconds.
- Headless consumer (`Profile Update Service`) belongs to consumer group `profile-update-group` with `auto_offset_reset='earliest'`, handling bursts without stressing the user API.

### 2. Flexible Document State in Relational Storage (PostgreSQL JSONB)
User tastes change dynamically across countless categories and tags:
- Stored in `users.preferences` as a `JSONB` document:
  ```json
  {
    "tech": 5,
    "gaming": 3,
    "books": 1
  }
  ```
- Incremented atomically via upsert on interaction:
  ```sql
  INSERT INTO users (user_id, preferences, updated_at)
  VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
  ON CONFLICT (user_id) DO UPDATE
  SET preferences = EXCLUDED.preferences,
      updated_at = CURRENT_TIMESTAMP;
  ```

### 3. Sub-millisecond Serving via Cache-Aside Pattern (Redis)
Recommendations are read-heavy:
1. **Cache Check**: Queries Redis for key `user:{user_id}:prefs`.
2. **Cache Hit**: Instantly deserializes the JSON vector and calculates recommendations.
3. **Cache Miss**: Queries PostgreSQL for the user's vector.
   - If user does not exist $\rightarrow$ Returns `404 Not Found`.
   - If user exists $\rightarrow$ Caches vector into Redis with a 1-hour TTL (`REDIS_TTL_SECONDS=3600`) and proceeds.

### 4. Session-Level Viewed Item Filtering
To prevent repeated recommendations to the same user:
- The Recommendation API tracks returned items per user using Redis sets (`user:{user_id}:viewed`) and an in-memory fallback.
- Newly recommended item IDs are filtered against this set and then appended to it.

---

## Quickstart Guide

### 1. Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)
- (Optional for host testing) Python 3.11+

### 2. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

### 3. Build & Launch the Services
Start all 6 containers with health checks:
```bash
docker compose up --build -d
```

Verify that all containers are healthy:
```bash
docker compose ps
```

All services will wait until PostgreSQL, Redis, and Kafka pass their health checks before booting.

---

## API Documentation

### Event Ingestion API (`http://localhost:8000`)

#### `POST /api/events/view`
Ingests a user interaction event.

**Request Body (JSON):**
```json
{
  "user_id": "user_1",
  "item_id": "item_1",
  "timestamp": "2026-10-16T12:00:00Z"
}
```

**Responses:**
- `202 Accepted`: Event validated and pushed to Kafka broker.
  ```json
  {
    "status": "accepted",
    "message": "Interaction event accepted for processing"
  }
  ```
- `400 Bad Request`: Missing or invalid fields (`user_id`, `item_id`, or ISO 8601 `timestamp`).
- `500 Internal Server Error`: Message broker temporarily unreachable.

---

### Recommendation API (`http://localhost:8001`)

#### `GET /api/recommendations/{user_id}`
Retrieves up to 5 personalized recommendations for a user.

**Responses:**
- `200 OK`:
  ```json
  {
    "user_id": "user_1",
    "recommendations": [
      "item_1",
      "item_2",
      "item_3",
      "item_9",
      "item_24"
    ]
  }
  ```
- `404 Not Found`: User ID not found in database.
  ```json
  {
    "detail": "User 'non_existent_user' not found in database"
  }
  ```

---

## Testing & Verification

### Run the Full Test Suite
Execute unit tests across all microservices and the end-to-end integration flow via:

**On Linux / macOS:**
```bash
chmod +x run_tests.sh
./run_tests.sh
```

**On Windows (PowerShell):**
```powershell
.\run_tests.ps1
```

### Run Unit Tests Individually
```bash
# Event Ingestion API
docker compose exec event_ingestion_api pytest -v tests/

# Profile Update Service
docker compose exec profile_update_service pytest -v tests/

# Recommendation API
docker compose exec recommendation_api pytest -v tests/
```

### Run the End-to-End Integration Flow
```bash
python scripts/e2e_test.py
```
This script tests:
1. Both service health checks.
2. 400 Bad Request rejection on invalid payload.
3. Event ingestion with 202 Accepted.
4. Asynchronous Kafka propagation and PostgreSQL state mutation.
5. Cache miss followed by Cache hit latency verification.
6. Viewed item exclusion logic across repeated calls.
7. 404 Not Found behavior for invalid users.

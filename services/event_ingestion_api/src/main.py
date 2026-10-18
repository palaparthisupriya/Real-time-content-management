import os
import json
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("event_ingestion_api")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_interactions")


class UserEvent(BaseModel):
    user_id: str = Field(..., min_length=1, description="Unique user identifier")
    item_id: str = Field(..., min_length=1, description="Unique item identifier")
    timestamp: datetime = Field(..., description="ISO 8601 formatted timestamp")


class KafkaProducerSingleton:
    """Thread-safe singleton managing the Kafka producer lifecycle."""
    _instance: Optional[KafkaProducer] = None

    @classmethod
    def get_producer(cls) -> KafkaProducer:
        if cls._instance is None:
            logger.info("Initializing KafkaProducer with bootstrap_servers=%s", KAFKA_BOOTSTRAP_SERVERS)
            try:
                cls._instance = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                    retries=3,
                    acks="all",
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=10000,
                )
                logger.info("KafkaProducer initialized successfully")
            except Exception as e:
                logger.error("Failed to initialize KafkaProducer: %s", str(e))
                raise
        return cls._instance

    @classmethod
    def set_producer(cls, producer: Optional[KafkaProducer]):
        """Allows injecting mock producer for unit testing."""
        cls._instance = producer

    @classmethod
    def close(cls):
        if cls._instance is not None:
            logger.info("Closing KafkaProducer connection...")
            try:
                cls._instance.flush(timeout=5)
                cls._instance.close(timeout=5)
            except Exception as e:
                logger.warning("Error while closing KafkaProducer: %s", str(e))
            finally:
                cls._instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Event Ingestion API...")
    try:
        KafkaProducerSingleton.get_producer()
    except Exception as e:
        logger.warning("Kafka broker not available at startup (%s). Will retry on requests.", str(e))
    yield
    # Shutdown
    logger.info("Shutting down Event Ingestion API...")
    KafkaProducerSingleton.close()


app = FastAPI(
    title="Event Ingestion API",
    description="Lightweight HTTP service accepting user interaction events and publishing to Kafka",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Override default 422 Unprocessable Entity with 400 Bad Request as required."""
    logger.warning("Validation error on request %s: %s", request.url.path, exc.errors())
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "Bad Request",
            "detail": exc.errors(),
            "message": "Missing or invalid required fields (user_id, item_id, timestamp ISO 8601)",
        },
    )


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy", "service": "event_ingestion_api"}


@app.post("/api/events/view", status_code=status.HTTP_202_ACCEPTED)
async def track_view_event(event: UserEvent):
    payload = {
        "user_id": event.user_id,
        "item_id": event.item_id,
        "timestamp": event.timestamp.isoformat(),
    }

    try:
        producer = KafkaProducerSingleton.get_producer()
        # Publish to Kafka
        future = producer.send(KAFKA_TOPIC, value=payload)
        logger.info("Enqueued event for user=%s item=%s to topic=%s", event.user_id, event.item_id, KAFKA_TOPIC)
    except KafkaError as ke:
        logger.error("Kafka error while publishing event: %s", str(ke), exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Broker Error", "message": "Failed to publish event to message queue"},
        )
    except Exception as ex:
        logger.error("Unexpected error publishing event: %s", str(ex), exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal Server Error", "message": "Failed to process event"},
        )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "accepted", "message": "Interaction event accepted for processing"},
    )

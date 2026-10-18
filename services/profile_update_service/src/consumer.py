import os
import json
import time
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("profile_update_service")

# Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_interactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "profile-update-group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "recommendation_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass")


def get_db_connection():
    """Establish and return a connection to PostgreSQL with retry logic."""
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            logger.info("Successfully connected to PostgreSQL at %s:%s", POSTGRES_HOST, POSTGRES_PORT)
            return conn
        except Exception as e:
            logger.warning("Database connection failed (%s). Retrying in 3 seconds...", str(e))
            time.sleep(3)


def parse_message(raw_value: bytes) -> Optional[Dict[str, Any]]:
    """
    Safely parse raw bytes from Kafka into a dictionary.
    Catches and logs invalid JSON without crashing.
    """
    try:
        if raw_value is None:
            logger.warning("Received empty message payload, skipping.")
            return None
        payload = json.loads(raw_value.decode("utf-8"))
        if not isinstance(payload, dict):
            logger.warning("Message payload is not a JSON object: %s", payload)
            return None
        # Validate essential keys
        if "user_id" not in payload or "item_id" not in payload:
            logger.warning("Malformed message missing required user_id or item_id: %s", payload)
            return None
        return payload
    except (json.JSONDecodeError, UnicodeDecodeError) as err:
        logger.error("Malformed message received: %s. Raw value: %s", str(err), raw_value)
        return None
    except Exception as ex:
        logger.error("Unexpected error parsing message: %s", str(ex))
        return None


def get_item_categories(cursor, item_id: str) -> List[str]:
    """Retrieve the categories list for a given item_id."""
    cursor.execute("SELECT categories FROM items WHERE item_id = %s;", (item_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        logger.warning("Item %s not found in items table or has no categories.", item_id)
        return []
    
    categories_raw = row[0]
    if isinstance(categories_raw, list):
        return [str(c).strip().lower() for c in categories_raw if c]
    elif isinstance(categories_raw, str):
        try:
            parsed = json.loads(categories_raw)
            if isinstance(parsed, list):
                return [str(c).strip().lower() for c in parsed if c]
        except Exception:
            return [categories_raw.strip().lower()]
    return []


def get_user_preferences(cursor, user_id: str) -> Dict[str, int]:
    """Retrieve the current preferences JSONB for a given user_id."""
    cursor.execute("SELECT preferences FROM users WHERE user_id = %s;", (user_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return {}
    
    prefs_raw = row[0]
    if isinstance(prefs_raw, dict):
        return prefs_raw
    elif isinstance(prefs_raw, str):
        try:
            return json.loads(prefs_raw)
        except Exception:
            return {}
    return {}


def calculate_updated_preferences(current_prefs: Dict[str, Any], categories: List[str]) -> Dict[str, int]:
    """
    Increment score for each category in the item.
    Example: {"tech": 2} with categories ["tech", "gadgets"] -> {"tech": 3, "gadgets": 1}
    """
    updated = {k: int(v) for k, v in current_prefs.items()}
    for category in categories:
        category_clean = category.strip().lower()
        if category_clean:
            updated[category_clean] = updated.get(category_clean, 0) + 1
    return updated


def save_user_preferences(conn, cursor, user_id: str, new_preferences: Dict[str, int]):
    """Upsert updated preferences into the users table."""
    query = """
        INSERT INTO users (user_id, preferences, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) DO UPDATE
        SET preferences = EXCLUDED.preferences,
            updated_at = CURRENT_TIMESTAMP;
    """
    cursor.execute(query, (user_id, Json(new_preferences)))
    conn.commit()


def process_event(conn, event: Dict[str, Any]) -> bool:
    """
    Process an event end-to-end:
    Lookup categories -> fetch user preferences -> recalculate -> persist.
    """
    user_id = event["user_id"]
    item_id = event["item_id"]
    logger.info("Processing interaction event: user=%s, item=%s", user_id, item_id)

    try:
        with conn.cursor() as cursor:
            # 1. Lookup item categories
            categories = get_item_categories(cursor, item_id)
            if not categories:
                logger.warning("No categories found for item=%s, preferences unchanged.", item_id)
                return True

            # 2. Get user preferences
            current_prefs = get_user_preferences(cursor, user_id)

            # 3. Recalculate preference vector
            new_prefs = calculate_updated_preferences(current_prefs, categories)
            logger.info("Updating preferences for user=%s: %s -> %s", user_id, current_prefs, new_prefs)

            # 4. Save to PostgreSQL
            save_user_preferences(conn, cursor, user_id, new_prefs)
            logger.info("Successfully updated preferences for user=%s in database.", user_id)
            return True
    except psycopg2.Error as pge:
        logger.error("PostgreSQL error while updating user %s: %s", user_id, str(pge))
        conn.rollback()
        return False
    except Exception as ex:
        logger.error("Unexpected error processing event for user %s: %s", user_id, str(ex))
        conn.rollback()
        return False


def start_consumer(max_messages: Optional[int] = None):
    """Continuously poll Kafka and process events."""
    logger.info("Initializing Profile Update Service Consumer...")
    logger.info("Connecting to Kafka at %s, topic=%s, group_id=%s",
                KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID)

    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                value_deserializer=lambda v: v,  # Raw bytes for safe parsing
            )
            logger.info("Kafka consumer connected successfully to %s", KAFKA_TOPIC)
        except KafkaError as ke:
            logger.warning("Kafka broker connection failed (%s). Retrying in 4 seconds...", str(ke))
            time.sleep(4)

    conn = get_db_connection()
    messages_processed = 0

    try:
        for message in consumer:
            try:
                # Safe JSON parsing
                event = parse_message(message.value)
                if event is not None:
                    # Verify DB connection liveness
                    if conn.closed != 0:
                        logger.warning("PostgreSQL connection closed. Reconnecting...")
                        conn = get_db_connection()

                    success = process_event(conn, event)
                    if not success:
                        logger.warning("Failed to process event for offset %s", message.offset)

                messages_processed += 1
                if max_messages and messages_processed >= max_messages:
                    logger.info("Reached maximum messages limit: %s", max_messages)
                    break
            except Exception as loop_ex:
                # Never crash the service on a single message failure
                logger.error("Error in consumer loop: %s", str(loop_ex), exc_info=True)
                continue
    finally:
        logger.info("Closing consumer and database connections...")
        if consumer:
            consumer.close()
        if conn and conn.closed == 0:
            conn.close()


if __name__ == "__main__":
    start_consumer()

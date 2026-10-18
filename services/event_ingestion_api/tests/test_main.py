import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from fastapi.testclient import TestClient

from src.main import app, KafkaProducerSingleton


class TestEventIngestionAPI(unittest.TestCase):
    def setUp(self):
        self.mock_producer = MagicMock()
        KafkaProducerSingleton.set_producer(self.mock_producer)
        self.client = TestClient(app)

    def tearDown(self):
        KafkaProducerSingleton.set_producer(None)

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "healthy", "service": "event_ingestion_api"})

    def test_track_view_event_success(self):
        payload = {
            "user_id": "user_123",
            "item_id": "item_456",
            "timestamp": "2026-10-16T10:00:00Z"
        }
        response = self.client.post("/api/events/view", json=payload)
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertEqual(data.get("status"), "accepted")

        # Verify producer send was called with user_interactions topic
        self.mock_producer.send.assert_called_once()
        args, kwargs = self.mock_producer.send.call_args
        self.assertEqual(args[0], "user_interactions")
        self.assertEqual(kwargs["value"]["user_id"], "user_123")
        self.assertEqual(kwargs["value"]["item_id"], "item_456")

    def test_track_view_event_missing_user_id(self):
        payload = {
            "item_id": "item_456",
            "timestamp": "2026-10-16T10:00:00Z"
        }
        response = self.client.post("/api/events/view", json=payload)
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data.get("error"), "Bad Request")

    def test_track_view_event_missing_item_id(self):
        payload = {
            "user_id": "user_123",
            "timestamp": "2026-10-16T10:00:00Z"
        }
        response = self.client.post("/api/events/view", json=payload)
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data.get("error"), "Bad Request")

    def test_track_view_event_invalid_timestamp(self):
        payload = {
            "user_id": "user_123",
            "item_id": "item_456",
            "timestamp": "not-a-valid-timestamp"
        }
        response = self.client.post("/api/events/view", json=payload)
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data.get("error"), "Bad Request")

    def test_track_view_event_kafka_error(self):
        self.mock_producer.send.side_effect = Exception("Kafka connection lost")
        payload = {
            "user_id": "user_123",
            "item_id": "item_456",
            "timestamp": "2026-10-16T10:00:00Z"
        }
        response = self.client.post("/api/events/view", json=payload)
        self.assertEqual(response.status_code, 500)
        data = response.json()
        self.assertIn("error", data)


if __name__ == "__main__":
    unittest.main()

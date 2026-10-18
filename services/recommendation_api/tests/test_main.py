import unittest
from unittest.mock import MagicMock, patch
import json
from fastapi.testclient import TestClient

from src.main import (
    app,
    ServiceClients,
    get_user_preferences_cache_aside,
    compute_recommendations,
    IN_MEMORY_VIEWED_ITEMS,
)


class TestRecommendationAPI(unittest.TestCase):
    def setUp(self):
        IN_MEMORY_VIEWED_ITEMS.clear()
        self.mock_redis = MagicMock()
        self.mock_db_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_db_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

        ServiceClients.redis_client = self.mock_redis
        ServiceClients.db_pool = MagicMock()
        ServiceClients.db_pool.getconn.return_value = self.mock_db_conn

        self.client = TestClient(app)

    def tearDown(self):
        ServiceClients.close_all()
        IN_MEMORY_VIEWED_ITEMS.clear()

    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "healthy", "service": "recommendation_api"})

    def test_cache_hit(self):
        # Redis has the key
        self.mock_redis.get.return_value = json.dumps({"tech": 5, "gadgets": 3})

        prefs = get_user_preferences_cache_aside("user_1", self.mock_redis, self.mock_db_conn)
        self.assertEqual(prefs, {"tech": 5, "gadgets": 3})
        # PostgreSQL should not be queried
        self.mock_cursor.execute.assert_not_called()

    def test_cache_miss_and_redis_population(self):
        # Redis miss
        self.mock_redis.get.return_value = None
        # DB hit
        self.mock_cursor.fetchone.return_value = ({"tech": 4, "gaming": 2},)

        prefs = get_user_preferences_cache_aside("user_1", self.mock_redis, self.mock_db_conn)
        self.assertEqual(prefs, {"tech": 4, "gaming": 2})

        # Verify DB was queried
        self.mock_cursor.execute.assert_called_once()
        # Verify Redis setex was called with TTL
        self.mock_redis.setex.assert_called_once()
        args = self.mock_redis.setex.call_args[0]
        self.assertEqual(args[0], "user:user_1:prefs")
        self.assertEqual(args[1], 3600)  # TTL
        self.assertEqual(json.loads(args[2]), {"tech": 4, "gaming": 2})

    def test_user_not_found_returns_404(self):
        # Redis miss
        self.mock_redis.get.return_value = None
        # DB miss
        self.mock_cursor.fetchone.return_value = None

        response = self.client.get("/api/recommendations/non_existent_user")
        self.assertEqual(response.status_code, 404)
        data = response.json()
        self.assertIn("not found", data["detail"].lower())

    def test_compute_recommendations_ranking(self):
        preferences = {"tech": 5, "sports": 1}
        viewed_items = set()

        # Mock items in DB:
        # item_1: tech (score 5)
        # item_2: sports (score 1)
        # item_3: tech + gadgets (score 5)
        # item_4: cooking (score 0)
        self.mock_cursor.fetchall.return_value = [
            ("item_1", ["tech"]),
            ("item_2", ["sports"]),
            ("item_3", ["tech", "gadgets"]),
            ("item_4", ["cooking"]),
        ]

        recommendations = compute_recommendations(preferences, viewed_items, self.mock_db_conn, limit=3)
        # item_1 and item_3 have higher score (5) than item_2 (1) and item_4 (0)
        self.assertIn("item_1", recommendations)
        self.assertIn("item_3", recommendations)
        self.assertIn("item_2", recommendations)
        self.assertNotIn("item_4", recommendations)

    def test_viewed_items_filtering(self):
        preferences = {"tech": 5}
        viewed_items = {"item_1"}  # Already viewed

        self.mock_cursor.fetchall.return_value = [
            ("item_1", ["tech"]),
            ("item_2", ["tech"]),
            ("item_3", ["tech"]),
        ]

        recommendations = compute_recommendations(preferences, viewed_items, self.mock_db_conn, limit=2)
        # item_1 must be excluded
        self.assertNotIn("item_1", recommendations)
        self.assertEqual(recommendations, ["item_2", "item_3"])

    def test_get_recommendations_endpoint_success(self):
        self.mock_redis.get.return_value = json.dumps({"tech": 5})
        self.mock_redis.smembers.return_value = set()
        self.mock_cursor.fetchall.return_value = [
            ("item_1", ["tech"]),
            ("item_2", ["tech"]),
        ]

        response = self.client.get("/api/recommendations/user_1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["user_id"], "user_1")
        self.assertEqual(data["recommendations"], ["item_1", "item_2"])


if __name__ == "__main__":
    unittest.main()

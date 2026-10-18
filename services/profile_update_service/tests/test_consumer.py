import unittest
from unittest.mock import MagicMock, patch
import json
import psycopg2

from src.consumer import (
    parse_message,
    calculate_updated_preferences,
    get_item_categories,
    get_user_preferences,
    process_event,
)


class TestProfileUpdateService(unittest.TestCase):
    def test_parse_message_valid(self):
        payload = {"user_id": "u1", "item_id": "i1", "timestamp": "2026-10-16T12:00:00Z"}
        raw = json.dumps(payload).encode("utf-8")
        result = parse_message(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["user_id"], "u1")
        self.assertEqual(result["item_id"], "i1")

    def test_parse_message_malformed_json(self):
        raw = b"{ malformed: json, missing quotes "
        # Should not throw exception, returns None
        result = parse_message(raw)
        self.assertIsNone(result)

    def test_parse_message_missing_required_fields(self):
        # Missing item_id
        raw = json.dumps({"user_id": "u1"}).encode("utf-8")
        result = parse_message(raw)
        self.assertIsNone(result)

        # Empty payload
        self.assertIsNone(parse_message(None))

    def test_calculate_updated_preferences_empty_initial(self):
        current_prefs = {}
        categories = ["tech", "gadgets"]
        new_prefs = calculate_updated_preferences(current_prefs, categories)
        self.assertEqual(new_prefs, {"tech": 1, "gadgets": 1})

    def test_calculate_updated_preferences_existing_vector(self):
        current_prefs = {"tech": 3, "books": 2}
        categories = ["tech", "sports"]
        new_prefs = calculate_updated_preferences(current_prefs, categories)
        self.assertEqual(new_prefs, {"tech": 4, "books": 2, "sports": 1})

    def test_get_item_categories_found(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (["tech", "computing"],)
        cats = get_item_categories(mock_cursor, "item_2")
        self.assertEqual(cats, ["tech", "computing"])

    def test_get_item_categories_not_found(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        cats = get_item_categories(mock_cursor, "non_existent")
        self.assertEqual(cats, [])

    def test_process_event_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock item categories
        mock_cursor.fetchone.side_effect = [
            (["tech", "gadgets"],),   # for items categories query
            ({"tech": 2},),           # for users preferences query
        ]

        event = {"user_id": "user_1", "item_id": "item_1", "timestamp": "2026-10-16T12:00:00Z"}
        result = process_event(mock_conn, event)

        self.assertTrue(result)
        mock_conn.commit.assert_called_once()

    def test_process_event_database_error(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.OperationalError("DB connection lost")

        event = {"user_id": "user_1", "item_id": "item_1", "timestamp": "2026-10-16T12:00:00Z"}
        # Must catch exception and return False without throwing
        result = process_event(mock_conn, event)
        self.assertFalse(result)
        mock_conn.rollback.assert_called_once()


if __name__ == "__main__":
    unittest.main()

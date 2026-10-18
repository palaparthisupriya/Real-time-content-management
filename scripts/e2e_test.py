#!/usr/bin/env python3
"""
End-to-End Integration Test for Real-Time Content Recommendation Service.
Tests cross-service interaction across Event Ingestion API, Kafka Broker,
Profile Update Service, PostgreSQL Database, Redis Cache, and Recommendation API.
"""

import os
import sys
import time
import json
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("Warning: 'requests' library not found. Please install via: pip install requests")
    sys.exit(1)

EVENT_API_URL = os.getenv("EVENT_API_URL", "http://localhost:8000")
RECOMMENDATION_API_URL = os.getenv("RECOMMENDATION_API_URL", "http://localhost:8001")


def print_step(msg: str):
    print(f"\n==========================================")
    print(f"--> {msg}")
    print(f"==========================================")


def test_health_checks():
    print_step("Step 1: Checking Service Health Endpoints")

    res_event = requests.get(f"{EVENT_API_URL}/health", timeout=5)
    print(f"Event Ingestion API /health: {res_event.status_code} {res_event.text}")
    assert res_event.status_code == 200, "Event Ingestion API is not healthy"

    res_rec = requests.get(f"{RECOMMENDATION_API_URL}/health", timeout=5)
    print(f"Recommendation API /health: {res_rec.status_code} {res_rec.text}")
    assert res_rec.status_code == 200, "Recommendation API is not healthy"
    print("PASS: Both API services are healthy.")


def test_validation_rejection():
    print_step("Step 2: Testing Input Validation (Expect 400 Bad Request)")

    invalid_payload = {
        "user_id": "test_user",
        # Missing item_id and timestamp
    }
    res = requests.post(f"{EVENT_API_URL}/api/events/view", json=invalid_payload, timeout=5)
    print(f"POST /api/events/view (invalid): Status {res.status_code}, Body: {res.text}")
    assert res.status_code == 400, f"Expected 400 Bad Request, got {res.status_code}"
    print("PASS: Malformed request rejected with 400 Bad Request.")


def test_event_ingestion_and_profile_update():
    print_step("Step 3: Ingesting User Interaction Event (202 Accepted)")

    test_user_id = "user_e2e_test"
    test_item_id = "item_1"  # Categories: ["tech", "gadgets"]
    now_iso = datetime.now(timezone.utc).isoformat()

    valid_payload = {
        "user_id": test_user_id,
        "item_id": test_item_id,
        "timestamp": now_iso
    }

    res = requests.post(f"{EVENT_API_URL}/api/events/view", json=valid_payload, timeout=5)
    print(f"POST /api/events/view (valid): Status {res.status_code}, Body: {res.text}")
    assert res.status_code == 202, f"Expected 202 Accepted, got {res.status_code}"
    print("PASS: Event ingested successfully.")

    print_step("Step 4: Waiting for Kafka & Profile Update Service processing...")
    # Give background consumer time to read from Kafka and update PostgreSQL
    time.sleep(4)


def test_recommendation_and_cache():
    print_step("Step 5: Testing Recommendation API & Cache-Aside (user_e2e_test)")

    test_user_id = "user_e2e_test"
    # Call 1: Should be Cache Miss -> Database query -> Redis population
    start_time = time.time()
    res1 = requests.get(f"{RECOMMENDATION_API_URL}/api/recommendations/{test_user_id}", timeout=5)
    duration1 = (time.time() - start_time) * 1000
    print(f"Call 1 (Cache Miss): Status {res1.status_code}, Time: {duration1:.2f}ms, Body: {res1.text}")
    assert res1.status_code == 200, f"Expected 200 OK, got {res1.status_code}"

    data1 = res1.json()
    assert data1["user_id"] == test_user_id
    assert isinstance(data1["recommendations"], list)
    assert len(data1["recommendations"]) > 0
    rec_items_1 = set(data1["recommendations"])

    # Call 2: Should hit Redis Cache and filter previously viewed items
    start_time = time.time()
    res2 = requests.get(f"{RECOMMENDATION_API_URL}/api/recommendations/{test_user_id}", timeout=5)
    duration2 = (time.time() - start_time) * 1000
    print(f"Call 2 (Cache Hit + Filtering): Status {res2.status_code}, Time: {duration2:.2f}ms, Body: {res2.text}")
    assert res2.status_code == 200

    data2 = res2.json()
    rec_items_2 = set(data2["recommendations"])

    # Ensure viewed items were filtered out (no overlap with Call 1)
    overlap = rec_items_1.intersection(rec_items_2)
    print(f"Overlap between Call 1 and Call 2: {overlap}")
    assert len(overlap) == 0, f"Viewed items filtering failed: found overlapping recommendations {overlap}"
    print("PASS: Recommendations served and viewed items filtered correctly.")


def test_non_existent_user():
    print_step("Step 6: Testing Non-Existent User (Expect 404 Not Found)")

    res = requests.get(f"{RECOMMENDATION_API_URL}/api/recommendations/non_existent_user_99999", timeout=5)
    print(f"GET /api/recommendations/non_existent_user_99999: Status {res.status_code}, Body: {res.text}")
    assert res.status_code == 404, f"Expected 404 Not Found, got {res.status_code}"
    print("PASS: 404 returned for non-existent user.")


def main():
    print("Starting End-to-End System Tests...")
    try:
        test_health_checks()
        test_validation_rejection()
        test_event_ingestion_and_profile_update()
        test_recommendation_and_cache()
        test_non_existent_user()
        print("\n==========================================")
        print(" ALL END-TO-END TESTS PASSED SUCCESSFULLY!")
        print("==========================================")
    except AssertionError as ae:
        print(f"\n[FAIL] Test assertion failed: {ae}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

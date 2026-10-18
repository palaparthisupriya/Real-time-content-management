"""
Verification script: fires 10 concurrent requests to a live running server
and prints response payloads, response times, and /metrics summary.
"""
import asyncio
import time
import httpx


BASE_URL = "http://localhost:8080"
KEY = "test_key_1"


async def fire_request(client: httpx.AsyncClient, req_id: int):
    start = time.perf_counter()
    resp = await client.get(f"{BASE_URL}/data/{KEY}")
    duration = time.perf_counter() - start
    print(f"Request {req_id:2d}: status={resp.status_code}, time={duration:.2f}s, body={resp.text}")
    return resp


async def main():
    print(f"--- Firing 10 concurrent requests for key '{KEY}' to {BASE_URL} ---")
    async with httpx.AsyncClient(timeout=10.0) as client:
        start_all = time.perf_counter()
        tasks = [fire_request(client, i) for i in range(1, 11)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_all
        print(f"\nAll 10 requests completed in {total_time:.2f}s (should be ~2.0s due to locking)")

        # Fetch metrics
        metrics_resp = await client.get(f"{BASE_URL}/metrics")
        print("\n--- Final Metrics ---")
        print(metrics_resp.json())


if __name__ == "__main__":
    asyncio.run(main())

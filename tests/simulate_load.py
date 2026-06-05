import asyncio
import time
import httpx

API_URL = "http://127.0.0.1:8000/api/v1/logs"
STATS_URL = "http://127.0.0.1:8000/api/v1/logs/stats"
NUM_LOGS = 1000

async def send_single_log(client: httpx.AsyncClient, i: int, semaphore: asyncio.Semaphore):
    payload = {
        "service_name": "load-generator",
        "level": "INFO" if i % 10 != 0 else "ERROR",
        "message": f"Simulation log number {i}",
        "metadata": {
            "index": i,
            "nested": {
                "tags": ["load-test", "simulation"],
                "active": True
            }
        }
    }
    async with semaphore:
        try:
            response = await client.post(API_URL, json=payload, timeout=10.0)
            return response.status_code == 202
        except Exception as e:
            print(f"Error sending log {i}: {type(e).__name__} - {e}")
            return False

async def main():
    print(f"Starting load simulation: sending {NUM_LOGS} logs to {API_URL}...")
    start_time = time.time()
    
    semaphore = asyncio.Semaphore(50)
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)) as client:
        # Send logs concurrently
        tasks = [send_single_log(client, i, semaphore) for i in range(NUM_LOGS)]
        results = await asyncio.gather(*tasks)
        
    end_time = time.time()
    success_count = sum(1 for r in results if r)
    duration = end_time - start_time
    
    print(f"\n--- Ingestion Phase Done ---")
    print(f"Sent: {NUM_LOGS}")
    print(f"Successful 202 responses: {success_count}/{NUM_LOGS}")
    print(f"Time taken: {duration:.2f} seconds")
    print(f"Ingestion rate: {success_count / duration:.2f} req/sec")
    
    print("\nWaiting 2.0 seconds for batch queue flushing to MongoDB...")
    await asyncio.sleep(2.0)
    
    # Query stats
    async with httpx.AsyncClient() as client:
        try:
            stats_resp = await client.get(STATS_URL)
            if stats_resp.status_code == 200:
                stats = stats_resp.json()
                print("\n--- Analytics Stats from MongoDB ---")
                print(f"Total Logs in DB: {stats.get('total_logs')}")
                print(f"Logs by Service: {stats.get('by_service')}")
                print(f"Logs by Level: {stats.get('by_level')}")
                print(f"Recent Errors: {len(stats.get('recent_errors', []))}")
            else:
                print(f"Failed to fetch stats: {stats_resp.status_code} - {stats_resp.text}")
        except Exception as e:
            print(f"Error fetching stats: {e}")

if __name__ == "__main__":
    asyncio.run(main())

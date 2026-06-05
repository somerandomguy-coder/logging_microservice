import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
import sys
from typing import Any, Dict, List
import httpx

# Helper to parse log line
def parse_log_line(line: str, default_service: str) -> Dict[str, Any]:
    line = line.strip()
    if not line:
        return {}
    
    # Try parsing as JSON
    try:
        data = json.loads(line)
        # Verify required or common fields
        service_name = data.get("service_name") or data.get("service") or default_service
        level = (data.get("level") or data.get("severity") or "INFO").upper()
        
        # Standardize level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in valid_levels:
            level = "INFO"
            
        message = data.get("message") or data.get("msg") or str(data)
        
        # Timestamp parsing
        ts_str = data.get("timestamp") or data.get("time") or data.get("ts")
        if ts_str:
            try:
                # Basic ISO parsing
                timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).isoformat()
            except Exception:
                timestamp = datetime.now(timezone.utc).isoformat()
        else:
            timestamp = datetime.now(timezone.utc).isoformat()
            
        # Metadata is everything else
        reserved_keys = {"service_name", "service", "level", "severity", "message", "msg", "timestamp", "time", "ts"}
        metadata = {k: v for k, v in data.items() if k not in reserved_keys}
        
        return {
            "service_name": service_name,
            "level": level,
            "timestamp": timestamp,
            "message": message,
            "metadata": metadata
        }
    except json.JSONDecodeError:
        # Fallback to plain text log parsing
        level = "INFO"
        upper_line = line.upper()
        for lvl in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            if lvl in upper_line:
                level = lvl
                break
                
        return {
            "service_name": default_service,
            "level": level,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": line,
            "metadata": {}
        }

async def send_batch(client: httpx.AsyncClient, url: str, batch: List[Dict[str, Any]]) -> bool:
    max_retries = 5
    backoff = 0.5
    for attempt in range(max_retries):
        try:
            response = await client.post(url, json=batch, timeout=5.0)
            if response.status_code == 202:
                print(f"[{datetime.now().isoformat()}] Successfully shipped {len(batch)} logs.")
                return True
            else:
                print(f"[{datetime.now().isoformat()}] Failed to ship batch. Server returned status {response.status_code}: {response.text}")
        except httpx.HTTPError as e:
            print(f"[{datetime.now().isoformat()}] Connection error shipping batch: {e}")
        
        if attempt < max_retries - 1:
            print(f"[{datetime.now().isoformat()}] Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff *= 2
    return False

async def tail_file_and_ship(file_path: str, url: str, service: str, batch_size: int, flush_interval: float):
    print(f"Starting Python Log Shipper...")
    print(f"Monitoring: {file_path}")
    print(f"Destination: {url}")
    print(f"Default Service: {service}")
    print(f"Batch Configuration: max_size={batch_size}, max_interval={flush_interval}s")
    
    # Wait for the file to exist if it doesn't
    while not os.path.exists(file_path):
        print(f"Target log file {file_path} not found. Waiting...")
        await asyncio.sleep(2.0)
        
    print(f"Found log file. Tailing starting from end of file.")
    
    # Keep track of current file size/position
    last_position = 0
    if os.path.exists(file_path):
        last_position = os.path.getsize(file_path)
        
    async with httpx.AsyncClient() as client:
        batch: List[Dict[str, Any]] = []
        last_flush = asyncio.get_event_loop().time()
        
        while True:
            if os.path.exists(file_path):
                current_size = os.path.getsize(file_path)
                if current_size > last_position:
                    with open(file_path, "r", encoding="utf-8") as f:
                        f.seek(last_position)
                        while True:
                            line = f.readline()
                            if not line:
                                break
                            parsed = parse_log_line(line, service)
                            if parsed:
                                batch.append(parsed)
                                if len(batch) >= batch_size:
                                    await send_batch(client, url, batch)
                                    batch = []
                                    last_flush = asyncio.get_event_loop().time()
                        last_position = f.tell()
                elif current_size < last_position:
                    # File was truncated/rotated
                    last_position = 0
            
            # Check flush interval
            now = asyncio.get_event_loop().time()
            if batch and (now - last_flush) >= flush_interval:
                await send_batch(client, url, batch)
                batch = []
                last_flush = now
                
            await asyncio.sleep(0.2)

def main():
    parser = argparse.ArgumentParser(description="Python Log Shipper Daemon")
    parser.add_argument("--file", required=True, help="Path to the log file to tail")
    parser.add_argument("--url", default="http://127.0.0.1:8000/api/v1/logs", help="FastAPI logging endpoint URL")
    parser.add_argument("--service", default="shipper-service", help="Default service name for parsed logs")
    parser.add_argument("--batch-size", type=int, default=20, help="Max batch size to ship")
    parser.add_argument("--flush-interval", type=float, default=1.0, help="Max wait time before shipping batch")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(tail_file_and_ship(
            file_path=args.file,
            url=args.url,
            service=args.service,
            batch_size=args.batch_size,
            flush_interval=args.flush_interval
        ))
    except KeyboardInterrupt:
        print("\nShipper stopped by user.")
        sys.exit(0)

if __name__ == "__main__":
    main()

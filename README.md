# Centralized Log Ingestion & Analytics Microservice

A high-performance, asynchronous logging microservice built with **FastAPI**, **PyMongo Async**, and **Redis**. It acts as a centralized log aggregator for distributed microservice systems, buffering incoming logs securely and persisting them to MongoDB optimized for time-series queries.

---

## 🏗️ System Architecture

```
[Client Services]               [Log Shipper CLI]
 (e.g. Payment, Auth)            (e.g. shipper.py)
         │                              │
         │                              │
         └───────────┬──────────────────┘
                     │ HTTP POST (X-API-Key)
                     ▼
             [FastAPI Ingest]
                     │
                     ▼ (RPUSH)
               [Redis Queue]
                     │
                     ▼ (BLPOP/LPOP Batching)
            [Async Batch Worker]
                     │
                     ▼ (Insert Many)
             [MongoDB Atlas]
         (TimeSeries & Text Index)
```

1. **FastAPI Ingestion Endpoint**: Accepts structured log payloads and returns `202 Accepted` immediately, offloading heavy processing from the calling client.
2. **Redis Persistent Queue**: Buffers logs durably in memory to guarantee no log loss on server crashes and handles backpressure.
3. **Async Batch Worker**: Retrieves items in batches from Redis and flushes them to MongoDB, reducing I/O write operations.
4. **MongoDB Time Series Collection**: Persists logs using native MongoDB Time Series collections optimized for time-stamped query speeds and automatic storage compression.
5. **Self-Cleaning Data Lifecycle**: Employs a Time-to-Live (TTL) index on MongoDB to automatically purge expired logs after 30 days.
6. **Optimized Search**: Leverages a MongoDB Text Index on log messages to support ultra-fast keyword searches.
7. **Zero-Dependency SDK Handler**: Packages an asynchronous logging handler `RemoteLogHandler` using standard library modules, easily droppable into any client application.

---

## 🚀 Quick Start Guide

### 1. Prerequisites (Docker)
Ensure Docker is running and launch MongoDB and Redis:
```bash
# Start MongoDB
docker run -d -p 27017:27017 --name local-mongo mongo:latest

# Start Redis
docker run -d -p 6379:6379 --name local-redis redis:latest
```

### 2. Install Dependencies
Initialize your virtual environment and install packages:
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run Automated Tests
Execute the pytest suite (verifying endpoints, batch worker, TTL, and Auth):
```bash
venv\Scripts\python -m pytest -v -s
```

### 4. Start the Log Ingest Server
Start the Uvicorn app server:
```bash
venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8000
```
The server will start listening at `http://127.0.0.1:8000`.

---

## 🧪 Simulation and Load Testing

### Load Simulation
With the FastAPI server running, execute the load simulation script to send **1,000 logs concurrently** and print database query analytics:
```bash
venv\Scripts\python tests\simulate_load.py
```

### File-Tailing Log Shipper
You can also run the file-tailing daemon to stream app logs to the microservice:
```bash
venv\Scripts\python shipper.py --file app.log --service payment-service
```

---

## 📦 Integrating the Remote Logger SDK

To use this logging service in another Python application, copy [remote_logger.py](remote_logger.py) to your project root. Integrate it with standard Python logging:

```python
import logging
import time
from remote_logger import RemoteLogHandler

# 1. Setup your logger
logger = logging.getLogger("BillingService")
logger.setLevel(logging.INFO)

# 2. Add the RemoteLogHandler
# Provide your endpoint and optionally X-API-Key header credentials
remote_handler = RemoteLogHandler(
    url="http://127.0.0.1:8000/api/v1/logs",
    api_key="your-api-key-here",  # Configure under API_KEYS env variable
    service_name="billing-service",
    batch_size=20,               # Ships when 20 logs accumulate...
    flush_interval=1.0           # ...or when 1.0s has passed
)
logger.addHandler(remote_handler)

# 3. Log normally!
logger.info("Payment invoice generated successfully", extra={"invoice_id": "inv_9932", "amount": 49.99})
logger.warning("Gateway API response time elevated")

try:
    raise ValueError("Invalid charge configuration")
except Exception:
    logger.exception("Failed to process invoice")

# Ensure background queue finishes shipping logs before exiting
time.sleep(1.5)
```
All logged events will automatically capture standard metadata (filename, line numbers, thread name, exceptions traceback) and ship asynchronously in the background.

---

## 🔒 Security Configuration
To protect logging endpoints:
1. Define the `API_KEYS` environment variable as a comma-separated list of values (e.g. `API_KEYS=key1,key2`).
2. Client log shippers must pass a valid key inside the `X-API-Key` request header.
3. If `API_KEYS` is left blank, the service operates in open mode (useful for local development).

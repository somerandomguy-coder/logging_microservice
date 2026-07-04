# 📝 Cloud Deployment Lessons & Reflections

This document serves as a reference guide detailing the architectural patterns, runtime errors, and key technical concepts encountered during the cloud deployment of the **Centralized Log Ingestion Microservice**.

---

## 🏗️ The Production Architecture (Serverless & Free-Tier)

We decoupled the logging system into three layers to keep the app lightweight and 100% free to run:

1. **Stateless Web/Worker (Fly.io)**: A Dockerized FastAPI application. Because it doesn't store databases or queue states locally, it easily runs on a `shared-cpu-1x` with `256MB` RAM.
2. **Persistent Queue (Upstash Redis)**: Used as a memory buffer to handle traffic spikes and backpressure.
3. **Document Store (MongoDB Atlas)**: Used as the permanent database optimized with **Time-Series collections** and **Text Indexes** for log searching.

---

## ⚠️ Key Deployment Errors & Troubleshooting

### 1. PyMongo DNS Resolution Failure (`mongodb+srv://`)
* **The Symptom**: When trying to connect the application to the MongoDB Atlas cluster (`mongodb+srv://...` connection string), connection attempts failed with a configuration error.
* **The Root Cause**: MongoDB Atlas uses DNS SRV records (the `+srv` part of the URI) to load-balance and query cluster nodes. By default, `pymongo` does not package the DNS resolution library.
* **The Fix**: Added `dnspython>=2.6.0` to `requirements.txt`.
* **The Lesson**: Always ensure `dnspython` is installed alongside PyMongo whenever you deploy to MongoDB Atlas.

### 2. Secure Redis Connection Scheme (`redis://` vs `rediss://`)
* **The Symptom**: Endless `Connection closed by server` errors logged from the batch queue worker, and `500 Internal Server Error` responses when posting log payloads to the endpoint.
* **The Root Cause**: Cloud Redis providers (like Upstash) enforce secure SSL/TLS connections in production. Connecting using the unencrypted protocol `redis://` gets blocked/dropped by the firewall.
* **The Fix**: Changed the `REDIS_URL` environment variable protocol scheme to **`rediss://`** (note the second `s` for secure).
* **The Lesson**: Cloud-based databases almost always require TLS. Ensure connection URIs explicitly request TLS/SSL using secure schemes (`rediss://`, `https://`, etc.).

### 3. Dynamic Port Binding on Fly.io / Docker
* **The Symptom**: If we hardcode uvicorn to port `8000` or `127.0.0.1` inside the container command, Fly.io cannot rout traffic to it and the deploy fails.
* **The Root Cause**: Fly.io (and other container platforms) dynamically injects a `PORT` environment variable to define which port the container must listen on.
* **The Fix**: Used a shell execution script in the `Dockerfile` CMD to respect the platform-injected port:
  ```dockerfile
  CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
  ```
* **The Lesson**: Never hardcode host/ports in production containers. Always bind to `0.0.0.0` (to accept external docker interface traffic) and dynamically bind to the platform's `PORT` env var.

---

## 🔒 Security & Client Ingestion Design

1. **Header Authentication**: Access is protected using the `X-API-Key` HTTP header.
2. **Open Mode vs Secure Mode**:
   - If `API_KEYS` is left blank, the app operates in open mode (ideal for local testing).
   - If `API_KEYS` is set (e.g. `16180339`), the `verify_api_key` dependency verifies headers before forwarding requests to the endpoint logic.
3. **Log Shipper Integration**:
   We modified [shipper.py](shipper.py) to accept the `--api-key` flag to dynamically forward authorization headers to the endpoint:
   ```bash
   python shipper.py --file mock_app.log --url https://logging-microservice.fly.dev/api/v1/logs --api-key <secret-key>
   ```

---

## 🧪 Best Practices for Ingestion Pipelines

* **Immediate Acknowledgement (202 Accepted)**: Ingestion routes should *never* block while waiting for database operations. Instead, they should validate input, throw it onto an asynchronous queue (Redis), and return a `202 Accepted` status immediately.
* **Batch Flushing**: A background task should pull items in batches (e.g. up to 100 logs at a time or every 1.0 second) and perform a single `insert_many` query in MongoDB, which reduces round-trips and database connection overhead.
* **Graceful Shutdown**: The FastAPI `lifespan` context manager ensures that if the server is stopped, the queue worker finishes flushing any remaining logs in Redis to MongoDB before the client/connection closes, preventing data loss.

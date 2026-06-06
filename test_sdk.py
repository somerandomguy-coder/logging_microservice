import logging
import time
from remote_logger import RemoteLogHandler

# 1. Setup the standard logger
logger = logging.getLogger("TestService")
logger.setLevel(logging.INFO)

# 2. Add the custom RemoteLogHandler pointing to your server
# If you configure API_KEYS in your .env, supply the key here: api_key="your-key"
remote_handler = RemoteLogHandler(
    url="http://127.0.0.1:8000/api/v1/logs",
    service_name="payment-gateway",
    batch_size=5,
    flush_interval=0.5
)
logger.addHandler(remote_handler)

# 3. Log messages normally!
print("Logging messages asynchronously...")
logger.info("Processing checkout transaction", extra={"transaction_id": "tx_abc123", "amount": 199.99})
logger.warning("Payment latency is higher than expected", extra={"latency_ms": 450})

try:
    1 / 0
except ZeroDivisionError:
    logger.exception("Failed to process payment receipt")

# Wait a brief moment to allow the background thread to ship remaining logs before exiting
time.sleep(1.0)
print("Finished!")
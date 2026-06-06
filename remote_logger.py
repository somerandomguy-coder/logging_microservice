import logging
import queue
import threading
import time
import json
import sys
import urllib.request
from datetime import datetime, timezone

class RemoteLogHandler(logging.Handler):
    """
    A zero-dependency, thread-safe logging handler that batches logs
    and ships them asynchronously in the background to the Centralized Log Microservice.
    """
    def __init__(
        self,
        url: str,
        api_key: str = None,
        service_name: str = "python-app",
        batch_size: int = 20,
        flush_interval: float = 1.0,
        level=logging.NOTSET
    ):
        super().__init__(level)
        self.url = url
        self.api_key = api_key
        self.service_name = service_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        # Thread-safe queue
        self.queue = queue.Queue()
        self.running = True
        
        # Background worker thread
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def emit(self, record):
        try:
            # Parse record into standard structured log payload dict
            payload = self._format_record(record)
            self.queue.put(payload)
        except Exception:
            self.handleError(record)

    def _format_record(self, record):
        level_map = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL"
        }
        level = level_map.get(record.levelno, "INFO")
        
        # Extract creation timestamp
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        
        # Build payload
        payload = {
            "service_name": self.service_name,
            "level": level,
            "timestamp": dt.isoformat(),
            "message": record.getMessage(),
            "metadata": {
                "logger_name": record.name,
                "filename": record.filename,
                "lineno": record.lineno,
                "funcName": record.funcName,
                "threadName": record.threadName
            }
        }
        
        # Add exception traceback to metadata if present
        if record.exc_info:
            payload["metadata"]["exception"] = self._format_exception(record.exc_info)
            
        # Merge extra attributes if provided on the logging call
        # Standard attributes are skipped
        standard_attrs = {
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'levelname', 'levelno', 'lineno', 'module',
            'msecs', 'message', 'msg', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName'
        }
        for key, val in record.__dict__.items():
            if key not in standard_attrs and not key.startswith('_'):
                payload["metadata"][key] = val
                
        return payload

    def _format_exception(self, exc_info):
        import traceback
        return "".join(traceback.format_exception(*exc_info))

    def _worker(self):
        batch = []
        last_flush = time.time()
        
        while self.running or not self.queue.empty():
            try:
                # Fetch next item from queue with a brief block timeout to let lifecycle checks run
                item = self.queue.get(timeout=0.1)
                batch.append(item)
                self.queue.task_done()
            except queue.Empty:
                pass
                
            now = time.time()
            # Flush if batch size or time interval threshold is hit
            if batch and (len(batch) >= self.batch_size or (now - last_flush) >= self.flush_interval):
                self._ship_batch(batch)
                batch = []
                last_flush = now
                
        # Final cleanup flush on service stop
        if batch:
            self._ship_batch(batch)

    def _ship_batch(self, batch):
        try:
            data = json.dumps(batch).encode("utf-8")
            req = urllib.request.Request(
                self.url,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    **({"X-API-Key": self.api_key} if self.api_key else {})
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=5.0) as response:
                # Validate response code
                if response.status not in (200, 202):
                    sys.stderr.write(f"[RemoteLogHandler] Failed to ship batch. Server status: {response.status}\n")
        except Exception as e:
            sys.stderr.write(f"[RemoteLogHandler] Exception shipping log batch: {str(e)}\n")

    def close(self):
        self.running = False
        self.worker_thread.join(timeout=2.0)
        super().close()

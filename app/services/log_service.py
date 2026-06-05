import asyncio
from typing import List, Dict, Any
import structlog
from app.config import settings
from app.database import get_collection

logger = structlog.get_logger()

class LogQueueManager:
    def __init__(self):
        self.queue: asyncio.Queue = None
        self.worker_task: asyncio.Task = None
        self.running: bool = False

    async def start(self):
        if self.running:
            return
        self.queue = asyncio.Queue()
        self.running = True
        self.worker_task = asyncio.create_task(self._batch_worker())
        logger.info("log_batch_worker_started", batch_size=settings.batch_size, batch_interval=settings.batch_interval)

    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info("stopping_log_batch_worker")
        
        # Cancel the active worker task, wait for it
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        
        # Flush whatever is left in the queue
        await self._flush_remaining()
        logger.info("log_batch_worker_stopped")

    async def enqueue(self, log_dict: Dict[str, Any]):
        await self.queue.put(log_dict)

    async def _batch_worker(self):
        while self.running:
            try:
                batch: List[Dict[str, Any]] = []
                # Wait for the first item in the batch
                try:
                    item = await self.queue.get()
                    batch.append(item)
                    self.queue.task_done()
                except asyncio.CancelledError:
                    break

                # We have the first item, now try to gather more until batch_size or batch_interval
                start_time = asyncio.get_event_loop().time()
                while len(batch) < settings.batch_size:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_time = settings.batch_interval - elapsed
                    if remaining_time <= 0:
                        break
                    
                    try:
                        # Non-blocking or timed wait for the next item
                        item = await asyncio.wait_for(self.queue.get(), timeout=remaining_time)
                        batch.append(item)
                        self.queue.task_done()
                    except asyncio.TimeoutError:
                        break
                    except asyncio.CancelledError:
                        # If cancelled during wait, we want to save what we have
                        break
                
                if batch:
                    await self._write_batch(batch)
            except Exception as e:
                logger.error("error_in_batch_worker", error=str(e))
                await asyncio.sleep(0.5)  # Prevents hot-looping on repeated errors

    async def _write_batch(self, batch: List[Dict[str, Any]]):
        collection = get_collection()
        if collection is None:
            logger.error("mongodb_collection_not_available", count=len(batch))
            return
        
        try:
            result = await collection.insert_many(batch)
            logger.info("flushed_logs_to_db", count=len(batch), inserted_ids=len(result.inserted_ids))
        except Exception as e:
            logger.error("failed_to_write_batch", error=str(e), count=len(batch))

    async def _flush_remaining(self):
        batch: List[Dict[str, Any]] = []
        while not self.queue.empty():
            item = self.queue.get_nowait()
            batch.append(item)
            self.queue.task_done()
        
        if batch:
            logger.info("flushing_remaining_logs_on_shutdown", count=len(batch))
            await self._write_batch(batch)

queue_manager = LogQueueManager()

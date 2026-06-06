import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any
import structlog
from app.config import settings
from app.database import get_collection

logger = structlog.get_logger()

class LogQueueManager:
    def __init__(self):
        self.redis = None
        self.worker_task: asyncio.Task = None
        self.running: bool = False

    async def start(self):
        if self.running:
            return
        from redis.asyncio import Redis
        # Connect to Redis
        self.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        self.running = True
        self.worker_task = asyncio.create_task(self._batch_worker())
        logger.info("log_batch_worker_started", batch_size=settings.batch_size, batch_interval=settings.batch_interval, redis_url=settings.redis_url)

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
        
        # Close Redis connection
        if self.redis:
            await self.redis.aclose()
            
        logger.info("log_batch_worker_stopped")

    async def enqueue(self, log_dict: Dict[str, Any]):
        if self.redis is None:
            logger.error("redis_not_available_for_enqueue")
            return
        log_json = json.dumps(log_dict)
        await self.redis.rpush(settings.redis_queue_name, log_json)

    async def get_queue_size(self) -> int:
        if self.redis:
            try:
                return await self.redis.llen(settings.redis_queue_name)
            except Exception as e:
                logger.error("failed_to_get_queue_size", error=str(e))
                return 0
        return 0

    async def _batch_worker(self):
        while self.running:
            try:
                batch: List[Dict[str, Any]] = []
                # Wait for the first item in the batch
                try:
                    res = await self.redis.blpop(settings.redis_queue_name, timeout=1.0)
                    if res:
                        _, log_json = res
                        log_dict = json.loads(log_json)
                        # Parse ISO timestamp string back into a datetime object for Time Series indexing in MongoDB
                        if "timestamp" in log_dict and isinstance(log_dict["timestamp"], str):
                            log_dict["timestamp"] = datetime.fromisoformat(log_dict["timestamp"].replace("Z", "+00:00"))
                        batch.append(log_dict)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("error_popping_from_redis", error=str(e))
                    await asyncio.sleep(0.5)
                    continue

                if not batch:
                    continue

                # Gather more logs up to batch_size
                start_time = asyncio.get_event_loop().time()
                while len(batch) < settings.batch_size:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_time = settings.batch_interval - elapsed
                    if remaining_time <= 0:
                        break
                    
                    try:
                        item = await self.redis.lpop(settings.redis_queue_name)
                        if item:
                            log_dict = json.loads(item)
                            if "timestamp" in log_dict and isinstance(log_dict["timestamp"], str):
                                log_dict["timestamp"] = datetime.fromisoformat(log_dict["timestamp"].replace("Z", "+00:00"))
                            batch.append(log_dict)
                        else:
                            # Queue is empty, sleep a tiny bit before retrying or exiting
                            await asyncio.sleep(0.05)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error("error_gathering_batch", error=str(e))
                        break
                
                if batch:
                    await self._write_batch(batch)
            except Exception as e:
                logger.error("error_in_batch_worker", error=str(e))
                await asyncio.sleep(0.5)

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
        if self.redis:
            try:
                while True:
                    item = await self.redis.lpop(settings.redis_queue_name)
                    if not item:
                        break
                    log_dict = json.loads(item)
                    if "timestamp" in log_dict and isinstance(log_dict["timestamp"], str):
                        log_dict["timestamp"] = datetime.fromisoformat(log_dict["timestamp"].replace("Z", "+00:00"))
                    batch.append(log_dict)
            except Exception as e:
                logger.error("error_flushing_remaining_from_redis", error=str(e))
        
        if batch:
            logger.info("flushing_remaining_logs_on_shutdown", count=len(batch))
            await self._write_batch(batch)

queue_manager = LogQueueManager()

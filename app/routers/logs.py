from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, Query, status
from app.database import get_collection
from app.models import LogLevel, LogPayload
from app.services.log_service import queue_manager
from app.auth import verify_api_key
import structlog

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/logs", tags=["logs"])

def serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc:
        return doc
    doc["id"] = str(doc["_id"])
    del doc["_id"]
    return doc

@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def ingest_log(
    payload: Union[LogPayload, List[LogPayload]],
    api_key: Optional[str] = Depends(verify_api_key)
):
    # Log ingestion: enqueue the data using Pydantic JSON serialization mode and return 202 Accepted
    if isinstance(payload, list):
        for item in payload:
            await queue_manager.enqueue(item.model_dump(mode="json"))
    else:
        await queue_manager.enqueue(payload.model_dump(mode="json"))
    return {"status": "accepted"}


@router.get("", response_model=List[Dict[str, Any]])
async def query_logs(
    service_name: Optional[str] = Query(None, description="Filter by service name"),
    level: Optional[LogLevel] = Query(None, description="Filter by log level"),
    start_time: Optional[datetime] = Query(None, description="Start timestamp filter (ISO format)"),
    end_time: Optional[datetime] = Query(None, description="End timestamp filter (ISO format)"),
    message: Optional[str] = Query(None, description="Keyword/phrase search using MongoDB Text Index"),
    limit: int = Query(50, ge=1, le=1000, description="Max logs to return"),
    skip: int = Query(0, ge=0, description="Number of logs to skip"),
    api_key: Optional[str] = Depends(verify_api_key)
):
    collection = get_collection()
    if collection is None:
        raise HTTPException(status_code=500, detail="Database connection not available")

    # Build the MongoDB filter query
    mongo_filter = {}
    if service_name:
        mongo_filter["service_name"] = service_name
    if level:
        mongo_filter["level"] = level.value
    if start_time or end_time:
        mongo_filter["timestamp"] = {}
        if start_time:
            mongo_filter["timestamp"]["$gte"] = start_time
        if end_time:
            mongo_filter["timestamp"]["$lte"] = end_time
            
    if message:
        # Use full-text index optimized query instead of slow regex substring scans
        mongo_filter["$text"] = {"$search": message}

    try:
        cursor = collection.find(mongo_filter).sort("timestamp", -1).skip(skip).limit(limit)
        results = await cursor.to_list(length=limit)
        return [serialize_doc(doc) for doc in results]
    except Exception as e:
        logger.error("error_querying_logs", error=str(e))
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")


@router.get("/stats", response_model=Dict[str, Any])
async def get_log_stats(api_key: Optional[str] = Depends(verify_api_key)):
    collection = get_collection()
    if collection is None:
        raise HTTPException(status_code=500, detail="Database connection not available")

    try:
        total_count = await collection.count_documents({})
        
        # Aggregation: Group by service_name
        service_pipeline = [
            {"$group": {"_id": "$service_name", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        service_cursor = await collection.aggregate(service_pipeline)
        services_stats = await service_cursor.to_list(length=100)
        
        # Aggregation: Group by level
        level_pipeline = [
            {"$group": {"_id": "$level", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        level_cursor = await collection.aggregate(level_pipeline)
        levels_stats = await level_cursor.to_list(length=100)

        # Get top 5 recent ERROR or CRITICAL logs
        recent_errors_cursor = collection.find(
            {"level": {"$in": ["ERROR", "CRITICAL"]}}
        ).sort("timestamp", -1).limit(5)
        recent_errors = await recent_errors_cursor.to_list(length=5)

        return {
            "total_logs": total_count,
            "by_service": {stat["_id"]: stat["count"] for stat in services_stats},
            "by_level": {stat["_id"]: stat["count"] for stat in levels_stats},
            "recent_errors": [serialize_doc(doc) for doc in recent_errors]
        }
    except Exception as e:
        logger.error("error_aggregating_stats", error=str(e))
        raise HTTPException(status_code=500, detail=f"Database aggregation error: {str(e)}")


@router.get("/stats/queue", response_model=Dict[str, Any])
async def get_queue_stats(api_key: Optional[str] = Depends(verify_api_key)):
    # Returns the current Redis queue size to monitor latency/backpressure
    size = await queue_manager.get_queue_size()
    return {
        "queue_length": size,
        "healthy": True
    }

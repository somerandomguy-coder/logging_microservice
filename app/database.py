from pymongo import AsyncMongoClient
from app.config import settings
import structlog

logger = structlog.get_logger()

class Database:
    client: AsyncMongoClient = None
    db = None
    collection = None

db_instance = Database()

async def connect_to_mongo():
    logger.info("connecting_to_mongodb", uri=settings.mongo_details)
    db_instance.client = AsyncMongoClient(settings.mongo_details)
    db_instance.db = db_instance.client[settings.mongo_db_name]
    
    db = db_instance.db
    collection_name = settings.mongo_collection_name
    collections = await db.list_collection_names()
    
    if collection_name not in collections:
        logger.info("creating_timeseries_collection", name=collection_name)
        try:
            await db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "service_name",
                    "granularity": "seconds"
                }
            )
        except Exception as e:
            logger.error("failed_creating_timeseries_collection", error=str(e))
            
    db_instance.collection = db[collection_name]
    
    # Create Indexes
    logger.info("creating_indexes")
    try:
        # TTL Index on timestamp (automatically manages time-series data deletion in MongoDB)
        await db_instance.collection.create_index(
            [("timestamp", 1)],
            expireAfterSeconds=settings.log_retention_days * 86400
        )
    except Exception as e:
        logger.error("failed_creating_ttl_index", error=str(e))
        
    try:
        # Text Index on message field for optimized search queries
        await db_instance.collection.create_index(
            [("message", "text")]
        )
    except Exception as e:
        logger.error("failed_creating_text_index", error=str(e))
        
    logger.info("connected_to_mongodb", db=settings.mongo_db_name, collection=collection_name)

async def close_mongo_connection():
    if db_instance.client:
        await db_instance.client.close()
        logger.info("closed_mongodb_connection")

def get_collection():
    return db_instance.collection

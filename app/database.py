from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
import structlog

logger = structlog.get_logger()

class Database:
    client: AsyncIOMotorClient = None
    db = None
    collection = None

db_instance = Database()

def connect_to_mongo():
    logger.info("connecting_to_mongodb", uri=settings.mongo_details)
    db_instance.client = AsyncIOMotorClient(settings.mongo_details)
    db_instance.db = db_instance.client[settings.mongo_db_name]
    db_instance.collection = db_instance.db[settings.mongo_collection_name]
    logger.info("connected_to_mongodb", db=settings.mongo_db_name, collection=settings.mongo_collection_name)

def close_mongo_connection():
    if db_instance.client:
        db_instance.client.close()
        logger.info("closed_mongodb_connection")

def get_collection():
    return db_instance.collection

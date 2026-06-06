from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.logging_config import setup_logging
from app.database import connect_to_mongo, close_mongo_connection
from app.services.log_service import queue_manager
from app.routers.logs import router as logs_router
import structlog

# Initialize structured logging
setup_logging()
logger = structlog.get_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    logger.info("server_starting")
    await connect_to_mongo()
    await queue_manager.start()
    yield
    # Shutdown actions
    logger.info("server_shutting_down")
    await queue_manager.stop()
    await close_mongo_connection()

app = FastAPI(
    title="Centralized Log Ingestion & Analytics API",
    description="A high-performance async logging microservice built with FastAPI, Motor, and MongoDB.",
    version="1.0.0",
    lifespan=lifespan
)

# Include APIs
app.include_router(logs_router)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

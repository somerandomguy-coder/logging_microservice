from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mongo_details: str = "mongodb://localhost:27017"
    mongo_db_name: str = "log_db"
    mongo_collection_name: str = "app_logs"
    batch_size: int = 100
    batch_interval: float = 1.0  # seconds
    app_host: str = "127.0.0.1"
    app_port: int = 8000

    class Config:
        env_file = ".env"

settings = Settings()

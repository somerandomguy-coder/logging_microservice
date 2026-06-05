from datetime import datetime
from enum import Enum
from typing import Any, Dict
from pydantic import BaseModel, Field

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class LogPayload(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=100)
    level: LogLevel = LogLevel.INFO
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    message: str = Field(..., min_length=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)

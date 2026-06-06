from fastapi import Header, HTTPException, status
from typing import Optional
from app.config import settings

def get_valid_api_keys() -> set[str]:
    if not settings.api_keys:
        return set()
    return {k.strip() for k in settings.api_keys.split(",") if k.strip()}

async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    valid_keys = get_valid_api_keys()
    # If no keys are configured, skip authentication (open service mode)
    if not valid_keys:
        return None
        
    if not x_api_key or x_api_key not in valid_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials. Invalid or missing API key."
        )
    return x_api_key

"""Rate limiter routes."""
import logging
import asyncio
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException, Depends, Response
from pydantic import BaseModel

from services.rate_limiter_service import RateLimiterService

logger = logging.getLogger(__name__)

router = APIRouter()

# Global service instance - will be set by app.py
_rate_limiter_service: Optional[RateLimiterService] = None

# Thread pool for blocking operations
_thread_pool = ThreadPoolExecutor(max_workers=10)


def set_rate_limiter_service(service: RateLimiterService):
    """Set the global service instance."""
    global _rate_limiter_service
    _rate_limiter_service = service


def get_rate_limiter_service() -> RateLimiterService:
    """Get rate limiter service instance."""
    if _rate_limiter_service is None:
        raise RuntimeError("Rate limiter service not initialized")
    return _rate_limiter_service


# Pydantic models
class TokenRequest(BaseModel):
    tokens: int
    bucket_id: str
    timeout: Optional[float]


class BucketConfig(BaseModel):
    bucket_id: str
    capacity: int
    refill_rate: float


@router.post("/buckets/init")
async def initialize_bucket(config: BucketConfig):
    """Initialize or update a token bucket.
    
    Args:
        config: Bucket configuration.
        
    Returns:
        dict: Initialization confirmation.
        
    Example:
        Request:
        ```json
        {
            "bucket_id": "default",
            "capacity": 128000,
            "refill_rate": 2133.33
        }
        ```
        
        Response:
        ```json
        {
            "status": "success",
            "bucket_id": "default",
            "capacity": 128000,
            "refill_rate": 2133.33
        }
        ```
    """
    try:
        service = get_rate_limiter_service()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _thread_pool,
            service.initialize_bucket,
            config.bucket_id,
            config.capacity,
            config.refill_rate
        )
        return result
    except Exception as e:
        logger.exception("Error initializing bucket")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/consume")
async def consume_tokens(request: TokenRequest, response: Response):
    """Consume tokens from bucket with optional timeout.
    
    Args:
        request: Token consumption request.
        response: Response object to add headers.
        
    Returns:
        dict: Consumption result with status, consumed, and remaining tokens.
        
    Example:
        Request:
        ```json
        {
            "tokens": 1000,
            "bucket_id": "default",
            "timeout": 60.0
        }
        ```
        
        Success Response:
        ```json
        {
            "status": "success",
            "consumed": 1000,
            "remaining": 127000
        }
        ```
        
        Error Response (429):
        ```json
        {
            "detail": "Rate limit timeout after 60.0s. Available: 500, Requested: 1000"
        }
        ```
    """
    try:
        service = get_rate_limiter_service()
        bucket_key = f"bucket:{request.bucket_id}"
        start_time = asyncio.get_event_loop().time()
        timeout = request.timeout if request.timeout else 60.0
        
        # Get bucket configuration for headers
        bucket_data = service._refill_bucket(bucket_key)
        capacity = int(float(bucket_data.get("capacity", 0)))
        
        while True:
            # Refill and check tokens
            bucket_data = service._refill_bucket(bucket_key)
            available_tokens = bucket_data["tokens"]
            
            if available_tokens >= request.tokens:
                # Consume tokens immediately
                service.redis_client.hset(
                    bucket_key,
                    "tokens",
                    available_tokens - request.tokens
                )
                remaining_tokens = available_tokens - request.tokens
                
                # Add rate limit headers
                response.headers["X-RateLimit-Limit"] = str(capacity)
                response.headers["X-RateLimit-Remaining"] = str(int(remaining_tokens))
                response.headers["X-RateLimit-Reset"] = str(int(asyncio.get_event_loop().time()))
                
                logger.debug(
                    f"Consumed {request.tokens} tokens from '{request.bucket_id}', "
                    f"{remaining_tokens:.0f} remaining"
                )
                return {
                    "status": "success",
                    "consumed": request.tokens,
                    "remaining": remaining_tokens
                }
            
            # Check timeout
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                logger.warning(
                    f"Timeout waiting for {request.tokens} tokens in '{request.bucket_id}'"
                )
                
                # Add rate limit headers even on error
                response.headers["X-RateLimit-Limit"] = str(capacity)
                response.headers["X-RateLimit-Remaining"] = str(int(available_tokens))
                response.headers["X-RateLimit-Reset"] = str(int(asyncio.get_event_loop().time()))
                
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit timeout after {elapsed:.1f}s. Available: {available_tokens}, Requested: {request.tokens}"
                )
            
            # Wait before retrying (async sleep doesn't block event loop)
            await asyncio.sleep(0.1)
            
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error consuming tokens")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tokens/available/{bucket_id}")
async def get_available_tokens(bucket_id: str, response: Response):
    """Get available tokens in bucket.
    
    Args:
        bucket_id: Identifier for the token bucket.
        response: Response object to add headers.
        
    Returns:
        dict: Bucket status with available tokens and configuration.
        
    Example:
        Response:
        ```json
        {
            "bucket_id": "default",
            "available_tokens": 125000,
            "capacity": 128000,
            "refill_rate": 2133.33
        }
        ```
    """
    try:
        service = get_rate_limiter_service()
        result = service.get_available_tokens(bucket_id)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(int(result.get("capacity", 0)))
        response.headers["X-RateLimit-Remaining"] = str(int(result.get("available_tokens", 0)))
        response.headers["X-RateLimit-Reset"] = str(int(asyncio.get_event_loop().time()))
        
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Error getting available tokens")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/buckets/{bucket_id}")
async def reset_bucket(bucket_id: str):
    """Reset a token bucket to full capacity.
    
    Args:
        bucket_id: Identifier for the token bucket.
        
    Returns:
        dict: Reset confirmation.
        
    Example:
        Response:
        ```json
        {
            "status": "success",
            "bucket_id": "default",
            "tokens": 128000
        }
        ```
    """
    try:
        service = get_rate_limiter_service()
        result = service.reset_bucket(bucket_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Error resetting bucket")
        raise HTTPException(status_code=500, detail=str(e))

"""Health check routes."""
import logging
from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()


def get_rate_limiter_service():
    """Get rate limiter service instance from app."""
    from app import rate_limiter_service
    return rate_limiter_service


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        service = get_rate_limiter_service()
        redis_connected = service.health_check()
        return {
            "status": "healthy" if redis_connected else "unhealthy",
            "service": "rate_limiter",
            "redis": "connected" if redis_connected else "disconnected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "rate_limiter",
            "error": str(e)
        }

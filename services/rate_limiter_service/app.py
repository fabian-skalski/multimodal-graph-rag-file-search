"""Rate Limiter FastAPI microservice with Redis backend."""
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.rate_limiter_service import RateLimiterService
from routes.rate_limiter_routes import router as rate_limiter_router, set_rate_limiter_service
from routes.health_routes import router as health_router
from middleware import SecurityHeadersMiddleware

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create single service instance
rate_limiter_service = RateLimiterService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan.
    
    Handles initialization and shutdown of the rate limiter service,
    including setting up the rate limiter service instance.
    
    Args:
        app: FastAPI application instance.
        
    Yields:
        None: Control to the application during its lifetime.
    """
    # Startup
    rate_limiter_service.initialize()
    set_rate_limiter_service(rate_limiter_service)
    logger.info("Rate limiter service started")
    yield
    # Shutdown (if needed)
    logger.info("Rate limiter service shutting down")


# Create FastAPI app
app = FastAPI(
    title="Rate Limiter Service",
    description="Distributed rate limiting service using token bucket algorithm",
    version="1.0.0",
    lifespan=lifespan
)

# Add security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# Include routers
app.include_router(health_router)
app.include_router(rate_limiter_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

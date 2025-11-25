"""Document Processing FastAPI microservice."""
import logging
import os

from fastapi import FastAPI

from routes.document_routes import router as document_router
from routes.health_routes import router as health_router
from middleware import SecurityHeadersMiddleware

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Create FastAPI app
app = FastAPI(
    title="Document Processing Service",
    description="Document chunking and processing",
    version="1.0.0"
)

# Add security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# Include routers
app.include_router(health_router)
app.include_router(document_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)

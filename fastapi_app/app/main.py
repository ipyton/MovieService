import logging
import os
import sys
import io
import datetime
import threading
from typing import List

from fastapi import FastAPI, Request, Response, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.api import api_router
from app.core.config import settings
from app.core.logging import configure_logger

# Configure stdout encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
configure_logger()
logger = logging.getLogger(__name__)

# Setup Prometheus metrics
Instrumentator().instrument(app).expose(app)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log incoming request information"""
    logger.info(f"Incoming request: {request.method} {request.url.path} from {request.client.host}")
    
    # Process the request
    start_time = datetime.datetime.now()
    response = await call_next(request)
    process_time = (datetime.datetime.now() - start_time).total_seconds() * 1000
    
    # Log response information
    logger.info(f"Response: {response.status_code} for {request.method} {request.url.path} - Took {process_time:.2f}ms")
    
    return response

@app.middleware("http")
async def authenticate(request: Request, call_next):
    """Authentication middleware"""
    # Skip authentication for certain paths
    if (request.method == 'OPTIONS' or 
        request.url.path == "/metrics" or
        request.url.path == "/health" or
        request.url.path.startswith("/docs") or
        request.url.path.startswith("/redoc") or
        request.url.path.startswith("/openapi")):
        return await call_next(request)
    
    # TODO: Implement authentication logic similar to the Flask app
    # This is a placeholder for the authentication middleware
    # For now, we'll just pass the request through
    
    return await call_next(request)

@app.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    return {"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}

@app.get("/metrics", tags=["metrics"])
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "An unexpected error occurred"}
    )

if __name__ == "__main__":
    import uvicorn
    
    logger.info("=" * 60)
    logger.info("Starting FastAPI application")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Environment: {os.environ.get('ENVIRONMENT', 'production')}")
    logger.info("=" * 60)
    
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 
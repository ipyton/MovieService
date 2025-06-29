from fastapi import APIRouter

from .endpoints import download, meta

api_router = APIRouter()

api_router.include_router(download.router, prefix="/movie", tags=["download"])
api_router.include_router(meta.router, prefix="/movie", tags=["meta"]) 
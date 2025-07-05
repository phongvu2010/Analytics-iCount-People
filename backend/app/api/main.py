from fastapi import APIRouter

from .routers import store #, crowd, error_log

api_router = APIRouter()
api_router.include_router(store.router, prefix = '/stores', tags = ['Stores'])
# api_router.include_router(crowd.router)
# api_router.include_router(error_log.router)

from fastapi import APIRouter

from .routers import data

api_router = APIRouter()
# api_router.include_router(login.router, tags = ["Login"])
# api_router.include_router(account.router, prefix = "/account", tags = ["Account"])

# api_router.include_router(auth.router, prefix='/api/v1', tags=['Authentication'])
api_router.include_router(data.router, prefix='num_crowd', tags=['Crowd Data'])
# api_router.include_router(logs.router, prefix='/api/v1', tags=['Error Logs'])
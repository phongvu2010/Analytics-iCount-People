# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import settings
# from .routers import store, crowd, error_log

app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    openapi_url = f'{settings.API_VERSION}/openapi.json',
    version = '1.1.0'
)

# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    origins = [
        str(origin).strip('/') for origin in settings.BACKEND_CORS_ORIGINS
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins = origins,        # Cho phép các origin trong danh sách
        allow_credentials = True,       # Cho phép gửi cookie
        allow_methods = ['*'],          # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers = ['*']           # Cho phép tất cả các header
    )

# # Include các routers
# app.include_router(store.router)
# app.include_router(crowd.router)
# app.include_router(error_log.router)
# app.include_router(store.router, prefix = f'{settings.API_VERSION}/stores', tags = ['Stores'])
# app.include_router(crowd.router, prefix = f'{settings.API_VERSION}/crowds', tags = ['Crowds Data'])
# app.include_router(error_log.router, prefix = f'{settings.API_VERSION}/errors', tags = ['Errors'])

@app.get('/', tags = ['Root'])
def read_root():
    return {
        'message': settings.DESCRIPTION,
        # 'sqlalchemy_database_uri': settings.SQLALCHEMY_DATABASE_URI
    }

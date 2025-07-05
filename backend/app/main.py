# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.main import api_router
from .core.config import settings

app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    openapi_url = f'{settings.API_VERSION}/openapi.json',
    version = '1.1.0'
)

# Set all CORS enabled origins - Cấu hình CORS để frontend có thể gọi API
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

# Thêm các router vào ứng dụng với tiền tố /api/v1
app.include_router(api_router, prefix = settings.API_VERSION)

@app.get('/', tags = ['Root'])
def read_root():
    return {
        'message': settings.DESCRIPTION,
        'sqlalchemy_database_uri': settings.SQLALCHEMY_DATABASE_URI
    }

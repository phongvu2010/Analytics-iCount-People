from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

# from .api.routers import data, logs, auth
from core.config import settings

app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    openapi_url = f"{settings.API_VERSION}/openapi.json",
    version = "1.1.0"
)

# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins = [
            str(origin).strip("/") for origin in settings.BACKEND_CORS_ORIGINS
        ],
        allow_credentials = True,
        allow_methods = ["*"],
        allow_headers = ["*"]
    )

# # Thêm các router vào ứng dụng với tiền tố /api/v1
# app.include_router(api_router, prefix = settings.API_V1_STR)
# app.include_router(auth.router, prefix='/api/v1', tags=['Authentication'])
# app.include_router(data.router, prefix='/api/v1', tags=['Crowd Data'])
# app.include_router(logs.router, prefix='/api/v1', tags=['Error Logs'])

@app.get('/', tags = ['Root'])
def read_root():
    return {
        'message': 'Welcome to iCount API!',
        # 'DATABASE_URI': settings.SQLALCHEMY_DATABASE_URI
    }

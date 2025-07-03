from fastapi import FastAPI
from .api.routers import data, logs, auth

from .core.config import settings

app = FastAPI(
    title='iCount People API',
    description='API for analyzing people traffic in stores.',
    version='1.1.0'
)

# # Thêm các router vào ứng dụng với tiền tố /api/v1
# app.include_router(auth.router, prefix='/api/v1', tags=['Authentication'])
# app.include_router(data.router, prefix='/api/v1', tags=['Crowd Data'])
# app.include_router(logs.router, prefix='/api/v1', tags=['Error Logs'])

@app.get('/', tags = ['Root'])
def read_root():
    return {
        'message': 'Welcome to iCount API!',
        # 'DATABASE_URI': settings.SQLALCHEMY_DATABASE_URI
    }

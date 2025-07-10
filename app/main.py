# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .core.config import settings
from .routers import router as api_router

app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='1.0.0'
)

# --- CORS Middleware Configuration ---
# FIXED: Ensure CORS is always enabled for development.
# This allows the frontend (even from a different origin) to make API calls to this backend.
origins = []
if settings.BACKEND_CORS_ORIGINS:
    # If the environment variable is set, use it.
    origins.extend([str(origin).strip('/') for origin in settings.BACKEND_CORS_ORIGINS])
else:
    # For local development, allow all origins.
    # In production, you should restrict this to your frontend's domain for security.
    origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,    # Cho phép các origin trong danh sách
    allow_credentials = True,   # Cho phép gửi cookie
    allow_methods = ['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
    allow_headers = ['*']       # Cho phép tất cả các header
)

# Mount thư mục static để phục vụ các file CSS, JS, images
app.mount('/static', StaticFiles(directory='app/statics'), name='static')

# Include router từ file routers.py
app.include_router(api_router)

@app.on_event('startup')
async def startup_event():
    """
    Kiểm tra kết nối database khi ứng dụng khởi động bằng SQLAlchemy engine.
    """
    from .core.db import engine

    try:
        print('Khởi động ứng dụng...')
        # engine.connect() sẽ thử tạo một kết nối tới DB
        conn = engine.connect()

        print('Kết nối CSDL qua SQLAlchemy thành công.')
        # Đóng kết nối ngay sau khi kiểm tra
        conn.close()
    except Exception as e:
        print(f'!!! LỖI: Không thể kết nối tới CSDL qua SQLAlchemy. Vui lòng kiểm tra file .env và kết nối mạng.')
        print(f'Chi tiết lỗi: {e}')

@app.get('/health')
def health_check():
    """
    Endpoint kiểm tra sức khoẻ của ứng dụng
    """
    return {'status': 'ok'}

# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .core.config import settings
from .routers import router as api_router

# Khởi tạo ứng dụng FastAPI với các thông tin từ file config
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='1.0.0'
)

# Cấu hình CORS Middleware
# Cho phép frontend (chạy trên domain khác) có thể gọi API của backend.
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

# Mount thư mục `static` để phục vụ các file: CSS, JS, Images
# FastAPI sẽ tìm file trong thư mục 'app/statics' khi có request tới '/static/...'
app.mount('/static', StaticFiles(directory='app/statics'), name='static')

# Include router từ file routers.py vào ứng dụng chính
# Tất cả các endpoint trong routers.py sẽ được thêm vào app
app.include_router(api_router)

@app.on_event('startup')
async def startup_event():
    """
    Sự kiện này sẽ chạy một lần khi ứng dụng khởi động.
    Rất hữu ích để kiểm tra kết nối CSDL.
    """
    from .core.db import engine

    try:
        print('Khởi động ứng dụng...')
        conn = engine.connect()
        print('Kết nối CSDL qua SQLAlchemy thành công.')
        conn.close()
    except Exception as e:
        print('!!! LỖI: Không thể kết nối tới CSDL qua SQLAlchemy.')
        print('Vui lòng kiểm tra file .env, kết nối mạng, và driver ODBC.')
        print(f'Chi tiết lỗi: {e}')

@app.get('/health', tags=["Health Check"])
def health_check():
    """
    Endpoint đơn giản để kiểm tra xem ứng dụng có đang chạy hay không.
    """
    return {'status': 'ok'}

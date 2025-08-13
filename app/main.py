"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

File này chịu trách nhiệm:
- Khởi tạo đối tượng FastAPI.
- Tích hợp các routers từ các module khác.
- Định nghĩa các endpoint chung (ví dụ: health-check).
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import settings
# Khởi tạo ứng dụng FastAPI.
api_app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    version = '1.0.0'
)

# Cấu hình CORS (Cross-Origin Resource Sharing) Middleware.
# Cho phép frontend từ các domain khác có thể gọi API này.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS]
    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,    # Cho phép các origin trong danh sách
        allow_credentials=True,   # Cho phép gửi cookie
        allow_methods=['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=['*']       # Cho phép tất cả các header
    )

@api_app.get('/health', tags=['Health Check'])
def health_check():
    """ Endpoint để kiểm tra tình trạng hoạt động của ứng dụng. """
    return {'status': 'ok'}

"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

File này chịu trách nhiệm:
- Khởi tạo đối tượng FastAPI.
- Tích hợp các routers từ các module khác.
- Định nghĩa các endpoint chung (ví dụ: health-check).
"""
from fastapi import FastAPI

from .api.routers import stores as stores_router

# Khởi tạo ứng dụng FastAPI với các thông tin mô tả
api_app = FastAPI(
    title='Analytics iCount People API',
    version='1.0.0',
    description='API cung cấp dữ liệu phân tích lượt ra vào cửa hàng.'
)

# --- Tích hợp router vào ứng dụng chính ---
api_app.include_router(stores_router.router, prefix='/api/v1')

@api_app.get('/', include_in_schema=False)
def read_root():
    """ Endpoint gốc của API. """
    return {'message': 'Chào mừng đến với Analytics iCount People API'}

"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

Tệp này chịu trách nhiệm:
- Khởi tạo đối tượng ứng dụng FastAPI.
- Cấu hình Middleware (ví dụ: CORS để cho phép frontend giao tiếp).
- Tích hợp các routers từ các module khác vào ứng dụng chính.
- Phục vụ các tệp tĩnh (CSS, JS, images) và template HTML cho giao diện.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router

# Khởi tạo ứng dụng FastAPI với các thông tin cơ bản từ config.
api_app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='2.1.0'
)

# Cấu hình Template Engine Jinja2, trỏ đến thư mục 'template'.
templates = Jinja2Templates(directory='template')

# Cấu hình CORS (Cross-Origin Resource Sharing) Middleware.
# Điều này là bắt buộc để trình duyệt cho phép trang web frontend
# (chạy trên một origin khác) gọi đến các API của backend này.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin).rstrip('/') for origin in settings.BACKEND_CORS_ORIGINS]
    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=['*'],  # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=['*']   # Cho phép tất cả các header.
    )

# Tích hợp Router API từ `app/routers.py` vào ứng dụng chính.
# Tất cả các endpoint trong `api_router` sẽ có tiền tố /api/v1.
api_app.include_router(api_router)

# Mount thư mục `statics` để phục vụ các tệp tĩnh.
# URL '/static' sẽ trỏ đến thư mục 'template/statics' trên server.
api_app.mount(
    '/static',
    StaticFiles(directory='template/statics'),
    name='static'
)


@api_app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def show_dashboard(request: Request):
    """
    Endpoint gốc (`/`), phục vụ trang dashboard.html cho người dùng.

    `include_in_schema=False` để không hiển thị endpoint này trong tài liệu API
    tự động (Swagger UI) vì nó chỉ trả về HTML.
    """
    return templates.TemplateResponse(
        'dashboard.html',
        {
            'request': request,
            'project_name': settings.PROJECT_NAME,
            'description': settings.DESCRIPTION
        }
    )


@api_app.get('/health', tags=['Health Check'])
def health_check():
    """
    Endpoint để kiểm tra tình trạng hoạt động (health status) của ứng dụng.
    Thường được sử dụng bởi các hệ thống monitoring hoặc load balancer.
    """
    return {'status': 'ok'}

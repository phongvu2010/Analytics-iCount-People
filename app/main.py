"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

Tệp này chịu trách nhiệm:
- Khởi tạo đối tượng FastAPI.
- Cấu hình Middleware (ví dụ: CORS).
- Tích hợp các routers từ các module khác.
- Phục vụ các tệp tĩnh và template HTML cho giao diện người dùng.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router

# Khởi tạo ứng dụng FastAPI với các thông tin cơ bản.
api_app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='2.1.0'
)

# Cấu hình Template Engine Jinja2, trỏ đến thư mục 'template'.
templates = Jinja2Templates(directory='template')

# Cấu hình CORS (Cross-Origin Resource Sharing) Middleware.
# Cho phép frontend từ các domain được chỉ định có thể gọi API này.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS]
    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,      # Cho phép các origin trong danh sách
        allow_credentials=True,     # Cho phép gửi cookie
        allow_methods=['*'],        # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=['*']         # Cho phép tất cả các header.
    )

# Tích hợp Router API từ `app/routers.py` vào ứng dụng chính.
api_app.include_router(api_router)

# Mount thư mục `statics` để phục vụ các tệp tĩnh (CSS, JS, Images).
# URL '/static' sẽ trỏ đến thư mục 'template/statics'.
api_app.mount('/static', StaticFiles(directory='template/statics'), name='static')


@api_app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def show_dashboard(request: Request):
    """
    Endpoint gốc, phục vụ trang dashboard.html cho người dùng.
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
    """
    return {'status': 'ok'}

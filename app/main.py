"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

File này chịu trách nhiệm:
- Khởi tạo đối tượng FastAPI.
- Tích hợp các routers từ các module khác.
- Định nghĩa các endpoint chung (ví dụ: health-check).
- Phục vụ các file tĩnh và template HTML cho dashboard.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router

# Khởi tạo ứng dụng FastAPI.
api_app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='2.1.0'
)

# Cấu hình Template Engine Jinja2. Trỏ đến thư mục 'template'
templates = Jinja2Templates(directory='template')

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

# Tích hợp Router API từ `app/routers.py` vào ứng dụng chính
api_app.include_router(api_router)

# Mount thư mục `statics` để phục vụ các tệp tĩnh (CSS, JS, Images).
# Dòng này sẽ mount thư mục 'template/statics' tại URL '/static'
# Ví dụ: file 'template/statics/logo.png' sẽ có thể được truy cập tại 'http://.../static/logo.png'
api_app.mount('/static', StaticFiles(directory='template/statics'), name='static')

# Tạo endpoint để hiển thị trong dashboard
@api_app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def show_dashboard(request: Request):
    """ Endpoint chính, phục vụ trang dashboard.html cho người dùng. """
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
    """ Endpoint để kiểm tra tình trạng hoạt động của ứng dụng. """
    return {'status': 'ok'}

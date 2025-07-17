# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router
from .utils.logger import setup_logging

# Khởi tạo ứng dụng FastAPI với các thông tin từ file config
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION
)

# Cấu hình CORS Middleware
# Cho phép frontend (chạy trên domain khác) có thể gọi API của backend.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,    # Cho phép các origin trong danh sách
        allow_credentials=True,   # Cho phép gửi cookie
        allow_methods=['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=['*']       # Cho phép tất cả các header
    )

# Mount thư mục `static` để phục vụ các file: CSS, JS, Images
# FastAPI sẽ tìm file trong thư mục `static` khi có request tới `/static/...`
app.mount('/static', StaticFiles(directory='static'), name='static')

# Cấu hình Jinja2 templates để phục vụ file HTML
# FastAPI sẽ tìm kiếm các file HTML trong thư mục `templates`
templates = Jinja2Templates(directory='templates')

# ======================================================================
# Endpoints API
# ======================================================================
app.include_router(
    api_router,
    prefix='/api/v1',       # Tiền tố cho tất cả các route trong router này
    tags=['Dashboard']      # Gắn tag để nhóm các API trong giao diện Swagger
)

@app.on_event('startup')
async def startup_event():
    # Có thể thực hiện các tác vụ khi khởi động ở đây
    # Ví dụ: khởi tạo kết nối, tải cache, v.v.
    # Setup logging khi ứng dụng khởi động
    setup_logging('FastAPI')
    logging.info('Application startup...')

@app.on_event('shutdown')
async def shutdown_event():
    # Dọn dẹp khi ứng dụng tắt
    logging.info('Application shutdown.')

@app.get('/health', tags=['Health Check'])
def health_check():
    """
    Endpoint đơn giản để kiểm tra xem ứng dụng có đang chạy hay không.
    """
    return {
        'status': 'ok'
    }

# Endpoint để phục vụ trang dashboard chính
@app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def read_root(request: Request):
    """
    Endpoint chính, phục vụ trang dashboard.
    """
    return templates.TemplateResponse(
        'dashboard.html', 
        {
            'request': request,
            'project_name': settings.PROJECT_NAME,
            'description': settings.DESCRIPTION
        }
    )

# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
import logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# from .api.routers import stores as stores_router
from .core.config import settings
from .routers import router as api_router
from .utils.logger import setup_logging

# Khởi tạo ứng dụng FastAPI.
api_app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    version = '1.0.0'
)

# --- Application Events ---
@api_app.on_event('startup')
async def startup_event():
    """Thiết lập logging khi ứng dụng khởi động."""
    setup_logging('FastAPI')
    logging.info('Application startup complete.')

@api_app.on_event('shutdown')
async def shutdown_event():
    """Ghi log khi ứng dụng tắt."""
    logging.info('Application shutdown.')

# --- Top-level Endpoints ---

# # Mount thư mục `static` để phục vụ các tệp tĩnh (CSS, JS, Images).
api_app.mount('/statics', StaticFiles(directory='template/statics'), name='static')

# Cấu hình Jinja2 để render các template HTML.
templates = Jinja2Templates(directory='template')

# --- Tích hợp router vào ứng dụng chính ---
api_app.include_router(
    api_router,
    prefix='/api/v1',     # Tiền tố cho tất cả các route trong router này
    tags=['Dashboard']    # Gắn tag để nhóm các API trong giao diện Swagger
)

@api_app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def read_root(request: Request):
    """ Phục vụ trang dashboard chính (dashboard.html). """
    return templates.TemplateResponse(
        'dashboard.html',
        {
            'request': request,
            'project_name': settings.PROJECT_NAME,
            'description': settings.DESCRIPTION
        }
    )

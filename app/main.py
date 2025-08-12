"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

File này chịu trách nhiệm:
- Khởi tạo đối tượng FastAPI.
- Tích hợp các routers từ các module khác.
- Định nghĩa các endpoint chung (ví dụ: health-check).
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# from .api.routers import stores as stores_router
from .core.config import etl_settings
from .routers import router as api_router

# Khởi tạo ứng dụng FastAPI.
api_app = FastAPI(
    title = etl_settings.PROJECT_NAME,
    description = etl_settings.DESCRIPTION,
    version = '1.0.0'
)

# Cấu hình CORS (Cross-Origin Resource Sharing) Middleware.
# Cho phép frontend từ các domain khác có thể gọi API này.
if etl_settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin) for origin in etl_settings.BACKEND_CORS_ORIGINS]
    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,    # Cho phép các origin trong danh sách
        allow_credentials=True,   # Cho phép gửi cookie
        allow_methods=['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=['*']       # Cho phép tất cả các header
    )

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

@api_app.get('/health', tags=['Health Check'])
def health_check():
    """ Endpoint để kiểm tra tình trạng hoạt động của ứng dụng. """
    return {'status': 'ok'}

@api_app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def read_root(request: Request):
    """ Phục vụ trang dashboard chính (dashboard.html). """
    return templates.TemplateResponse(
        'dashboard.html',
        {
            'request': request,
            'project_name': etl_settings.PROJECT_NAME,
            'description': etl_settings.DESCRIPTION
        }
    )









# # Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# # Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
# import logging

# from .utils.logger import setup_logging

# # --- Application Events ---
# @app.on_event('startup')
# async def startup_event():
#     """Thiết lập logging khi ứng dụng khởi động."""
#     setup_logging('FastAPI')
#     logging.info('Application startup complete.')

# @app.on_event('shutdown')
# async def shutdown_event():
#     """Ghi log khi ứng dụng tắt."""
#     logging.info('Application shutdown.')

# # --- Top-level Endpoints ---

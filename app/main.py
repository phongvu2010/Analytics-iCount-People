from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .api.v1 import errors
from .core import settings

# Khởi tạo ứng dụng FastAPI
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='1.0.0'
)

# Cấu hình CORS
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials = True,
        allow_methods = ['*'],
        allow_headers = ['*']
    )

# Mount thư mục static để phục vụ file CSS, JS, images
app.mount('/static', StaticFiles(directory = 'app/static'), name = 'static')

# Cấu hình Jinja2 templates
templates = Jinja2Templates(directory = 'app/templates')

# Mount các router API
# app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(errors.router, prefix = '/api/v1/errors', tags = ['Errors'])













# =====================================================================================
# from fastapi import Request
# from fastapi.responses import HTMLResponse

# from app.api.v1 import analytics

# @app.get("/", response_class=HTMLResponse)
# async def read_root(request: Request):
#     """
#     Endpoint chính, phục vụ trang dashboard.
#     """
#     return templates.TemplateResponse(
#         "dashboard.html", 
#         {
#             "request": request,
#             "project_name": settings.PROJECT_NAME,
#             "description": settings.DESCRIPTION
#         }
#     )

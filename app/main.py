# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI     # Request, responses
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse, RedirectResponse
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates

from .core import settings
# from .api.v1 import errors  # analytics, 

# Khởi tạo ứng dụng FastAPI với các thông tin từ file config
app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    version = '1.0.0'
)

# Cấu hình CORS Middleware
# Cho phép frontend (chạy trên domain khác) có thể gọi API của backend.
origins = []
if settings.BACKEND_CORS_ORIGINS:
    # If the environment variable is set, use it.
    origins.extend([str(origin) for origin in settings.BACKEND_CORS_ORIGINS])
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
# FastAPI sẽ tìm file trong thư mục `app/static` khi có request tới `/static/...`
# app.mount('/static', StaticFiles(directory = 'app/static'), name = 'static')

# Cấu hình Jinja2 templates để phục vụ file HTML
# FastAPI sẽ tìm kiếm các file HTML trong thư mục `app/templates`
# templates = Jinja2Templates(directory = 'app/templates')

# # Mount các API Routers
# app.include_router(api_router, prefix = settings.API_V1_STR)
# app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
# app.include_router(errors.router, prefix = '/api/v1/errors', tags = ['Errors'])

# =============================================
# Endpoints để phục vụ giao diện
# =============================================
@app.get('/health', tags = ['Health Check'])
def health_check():
    """
    Endpoint đơn giản để kiểm tra xem ứng dụng có đang chạy hay không.
    """
    # df = get_all_stores()
    # print(df)
    # print(df.dtypes)
    # print(type(df))
    return {
        'status': 'ok',
        'CROWD_COUNTS_PATH': settings.CROWD_COUNTS_PATH
    }









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

# # --- Include  ---
# # Gắn các API endpoints từ module analytics vào ứng dụng chính
# app.include_router(
#     analytics.router, 
#     prefix="/api/v1/analytics", # Tiền tố cho tất cả các route trong router này
#     tags=["Analytics"]          # Gắn tag để nhóm các API trong giao diện Swagger
# )

# # --- Root Redirect ---
# @app.get("/", include_in_schema=False)
# async def root_redirect():
#     """
#     Khi truy cập vào đường dẫn gốc, tự động chuyển hướng đến trang dashboard.
#     """
#     return RedirectResponse(url="/api/v1/analytics/dashboard")


# ================================================================================================
# from .routers import router as api_router

# # Include router từ file routers.py vào ứng dụng chính
# # Tất cả các endpoint trong routers.py sẽ được thêm vào app
# app.include_router(api_router)

# @app.on_event('startup')
# async def startup_event():
#     """
#     Sự kiện này sẽ chạy một lần khi ứng dụng khởi động.
#     Rất hữu ích để kiểm tra kết nối CSDL.
#     """
#     from .core.db import engine

#     try:
#         print('Khởi động ứng dụng...')
#         conn = engine.connect()
#         print('Kết nối CSDL qua SQLAlchemy thành công.')
#         conn.close()
#     except Exception as e:
#         print('!!! LỖI: Không thể kết nối tới CSDL qua SQLAlchemy.')
#         print('Vui lòng kiểm tra file .env, kết nối mạng, và driver ODBC.')
#         print(f'Chi tiết lỗi: {e}')

# @app.get('/', include_in_schema = False)
# async def read_root():
#     """
#     Redirect từ URL gốc (/) sang trang dashboard.
#     """
#     return responses.RedirectResponse(url='/dashboard')

# @app.get('/dashboard', response_class = responses.HTMLResponse, include_in_schema=False)
# async def get_dashboard(request: Request):
#     """
#     Phục vụ file dashboard.html từ thư mục templates.
#     """
#     return templates.TemplateResponse('dashboard.html', {'request': request})

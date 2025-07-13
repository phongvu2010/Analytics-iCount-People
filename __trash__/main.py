from fastapi import Request, responses
# from fastapi.responses import HTMLResponse, RedirectResponse

# from .api.v1 import errors  # analytics, 

# # Mount các API Routers
# app.include_router(api_router, prefix = settings.API_V1_STR)
# app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
# app.include_router(errors.router, prefix = '/api/v1/errors', tags = ['Errors'])








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

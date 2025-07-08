from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Import các router từ thư mục routers
from .config import settings
# from .routers import auth, dashboard

# Khởi tạo ứng dụng FastAPI
app = FastAPI(title=settings.PROJECT_NAME, description=settings.DESCRIPTION, version='1.0.0')

# 1. Cấu hình để phục vụ các file tĩnh (CSS, JS, Images)
# Các file trong thư mục 'app/static' sẽ được truy cập qua đường dẫn '/static'
app.mount('/static', StaticFiles(directory='app/static'), name='static')

# 2. Cấu hình Jinja2 templates
# FastAPI sẽ tìm kiếm các file HTML trong thư mục 'app/templates'
templates = Jinja2Templates(directory='app/templates')

# 3. "Lắp ráp" các router vào ứng dụng chính
# Bao gồm các endpoint từ file auth.py và dashboard.py
# app.include_router(auth.router, tags=['Authentication'])
# app.include_router(dashboard.router, tags=['Dashboard'])


# 4. Tạo một route gốc để chuyển hướng người dùng
@app.get('/', include_in_schema=False)
async def root(request: Request):
    """
    Khi người dùng truy cập vào đường dẫn gốc,
    hệ thống sẽ tự động chuyển hướng họ đến trang đăng nhập.
    """
    return RedirectResponse(url='/login')

# Lời khuyên: Để chạy ứng dụng, bạn sẽ mở terminal,
# di chuyển vào thư mục gốc 'mall_traffic_analysis' và chạy lệnh:
# uvicorn app.main:app --reload
#
# --reload: Tự động khởi động lại server mỗi khi có thay đổi trong code.

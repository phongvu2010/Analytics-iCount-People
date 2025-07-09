# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from datetime import datetime, timedelta, date
from fastapi import FastAPI, Request, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional

from . import crud
from .core.config import settings
from .core.database import get_db

# --- App Initialization ---
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version='1.0.0'
)

# --- CORS Middleware Configuration ---
# FIXED: Ensure CORS is always enabled for development.
# This allows the frontend (even from a different origin) to make API calls to this backend.
origins = []
if settings.BACKEND_CORS_ORIGINS:
    # If the environment variable is set, use it.
    origins.extend([str(origin).strip('/') for origin in settings.BACKEND_CORS_ORIGINS])
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

# 1. Cấu hình để phục vụ các file tĩnh (CSS, JS, Images)
# Các file trong thư mục 'app/static' sẽ được truy cập qua đường dẫn '/static'
app.mount('/static', StaticFiles(directory='static'), name='static')
# app.mount('/static', StaticFiles(directory='app/static'), name='static')

# # 2. Cấu hình Jinja2 templates
# # FastAPI sẽ tìm kiếm các file HTML trong thư mục 'app/templates'
templates = Jinja2Templates(directory='templates')
# templates = Jinja2Templates(directory='app/templates')

@app.get('/health', tags = ['Root'])
def read_root():
    """ Health check endpoint. """
    return {
        'status': 'ok',
        'project_name': f'Welcome to {settings.PROJECT_NAME}',
        'description': settings.DESCRIPTION,
        'backend_cors_origins': origins,
        'sqlalchemy_database_uri': settings.SQLALCHEMY_DATABASE_URI
    }

@app.get('/')
async def read_root(request: Request):
    """ Endpoint chính, render trang dashboard. """
    return templates.TemplateResponse('dashboard.html', {'request': request})

@app.get('/api/data')
async def get_dashboard_data(
    period: str = Query('day', enum=['day', 'week', 'month', 'year']),
    store_id: Optional[int] = Query(None),
    selected_date_str: Optional[str] = Query(None), # YYYY-MM-DD
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    week: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    """ API cung cấp toàn bộ dữ liệu cho dashboard. """
    now = datetime.now()

    # Xác định khoảng thời gian hiện tại
    if period == 'day':
        current_day = datetime.strptime(selected_date_str, '%Y-%m-%d') if selected_date_str else now
        start_date = current_day.replace(hour=0, minute=0, second=0)
        end_date = current_day.replace(hour=23, minute=59, second=59)
    # Các logic xác định start_date, end_date cho week, month, year tương tự...
    # (Code chi tiết sẽ phức tạp, ở đây ta giả định đã có)

    # Lấy dữ liệu thô từ CSDL
    # Trong thực tế, bạn sẽ cần logic phức tạp hơn để xử lý các bộ lọc
    # Ở đây, chúng ta sẽ mô phỏng việc lấy dữ liệu của 7 ngày gần nhất để tính toán
    end_date = now
    start_date = now - timedelta(days=7)

    all_data = crud.get_crowd_data_in_range(db, start_date, end_date)

    # Lọc theo cửa hàng nếu có
    if store_id:
        all_data = [d for d in all_data if d.storeid == store_id]

    # --- Xử lý dữ liệu để tạo các số liệu ---
    # Đây là phần logic Data Analysis chính
    # Do phức tạp, phần này sẽ được mô phỏng. Trong thực tế, bạn sẽ
    # dùng các thư viện như Pandas hoặc các câu lệnh SQL phức tạp.

    # 1. Dữ liệu cho Line Chart (giả định theo ngày)
    labels = [(end_date - timedelta(days=i)).strftime('%d/%m') for i in range(6, -1, -1)]
    line_data = [d.in_num for d in all_data[:7]] if len(all_data) >= 7 else [150, 220, 300, 250, 400, 380, 500]

    # 2. Dữ liệu cho Donut Chart
    stores = crud.get_stores(db)
    store_traffic = {store.name: 0 for store in stores}
    for record in all_data:
        store_name = next((s.name for s in stores if s.tid == record.storeid), 'Không rõ')
        store_traffic[store_name] += record.in_num

    donut_labels = list(store_traffic.keys())
    donut_data = list(store_traffic.values())

    # 3. Các số liệu (Metrics)
    total_in = sum(d.in_num for d in all_data)
    average_in = total_in / len(all_data) if all_data else 0

    # 4. Lấy lỗi
    error_logs = crud.get_error_logs(db)

    return {
        'line_chart_data': {'labels': labels, 'data': line_data},
        'donut_chart_data': {'labels': donut_labels, 'data': donut_data},
        'table_data': {'labels': labels, 'data': line_data},
        'metrics': {
            'total_in': total_in,
            'average_in': average_in,
            'peak_time': '19:00', # Giả định
            'occupancy': total_in - sum(d.out_num for d in all_data),
            'busiest_store': max(store_traffic, key=store_traffic.get) if store_traffic else '--',
            'growth': 15.2 # Giả định
        },
        'error_logs': error_logs,
        'stores': stores
    }







# import uvicorn

# # from fastapi.responses import RedirectResponse, HTMLResponse
# # from pathlib import Path

# import crud, models, schemas
# from database import engine


# from app.core.database import engine, get_db
# from .routers import auth, crowd, dashboard, error_log, store


# # Tạo các bảng trong CSDL nếu chưa có (chỉ cho lần chạy đầu)
# models.Base.metadata.create_all(bind=engine)

# # Lệnh để chạy server: uvicorn main:app --reload
# if __name__ == "__main__":
#     # Thêm dữ liệu mẫu vào CSDL khi chạy lần đầu (nếu cần)
#     db = SessionLocal()
#     if not db.query(models.Store).first():
#         print("Đang thêm dữ liệu mẫu...")
#         stores_to_add = [
#             models.Store(tid=1, name='Cửa chính A1'),
#             models.Store(tid=2, name='Cửa phụ A2'),
#             models.Store(tid=3, name='Cửa hầm B1'),
#             models.Store(tid=4, name='Cửa hầm B2')
#         ]
#         db.add_all(stores_to_add)
        
#         crowd_data_to_add = []
#         for i in range(30 * 24): # Dữ liệu 30 ngày
#             for store in stores_to_add:
#                 record_time = datetime.now() - timedelta(hours=i)
#                 in_num = 50 + (i % 24) * 5 + (-1)**i * 10
#                 out_num = int(in_num * 0.8)
#                 crowd_data_to_add.append(models.NumCrowd(
#                     recordtime=record_time, storeid=store.tid, in_num=in_num, out_num=out_num
#                 ))
#         db.bulk_save_objects(crowd_data_to_add)
#         db.commit()
#     db.close()
    
#     uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)




# # 3. "Lắp ráp" các router vào ứng dụng chính
# # Bao gồm các endpoint từ file auth.py và dashboard.py
# # app.include_router(auth.router, tags=['Authentication'])
# # app.include_router(dashboard.router, tags=['Dashboard'])

# # 4. Tạo một route gốc để chuyển hướng người dùng
# @app.get('/', include_in_schema=False)
# async def root(request: Request):
#     """
#     Khi người dùng truy cập vào đường dẫn gốc,
#     hệ thống sẽ tự động chuyển hướng họ đến trang đăng nhập.
#     """
#     return RedirectResponse(url='/login')

# # --- Static Files and Templates ---
# # This assumes your directory structure is:
# # iCount-People-Project/
# # ├── app/
# # │   ├── main.py
# # │   ├── templates/
# # │   │   └── index.html
# # │   └── static/ (optional, if you have local css/js)
# # └── .env
# BASE_DIR = Path(__file__).resolve().parent
# app.mount('/static', StaticFiles(directory = str(Path(BASE_DIR, 'static'))), name = 'static')
# templates = Jinja2Templates(directory=str(Path(BASE_DIR, 'templates')))

# # --- API Routers ---
# # Note: The trailing slash in the prefix is optional but can help avoid 307 redirects.
# # The frontend code has already been updated to include it.
# app.include_router(store.router, prefix = '/api/stores', tags = ['Stores'])
# app.include_router(crowd.router, prefix = '/api/crowds', tags = ['Crowds Data'])
# app.include_router(error_log.router, prefix = '/api/errors', tags = ['Errors'])

# # --- Frontend Route ---
# @app.get('/', response_class = HTMLResponse, tags = ['Frontend'])
# async def read_dashboard(request: Request):
#     """ Serves the main dashboard HTML page. """
#     return templates.TemplateResponse(
#         'index.html',
#         {
#             'request': request,
#             'project_name': settings.PROJECT_NAME
#         }
#     )

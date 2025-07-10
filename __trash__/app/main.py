# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from collections import defaultdict
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
# Các file trong thư mục 'app/static' sẽ được truy cập qua đường dẫn 'app/static'
app.mount('/static', StaticFiles(directory='app/static'), name='static')

# # 2. Cấu hình Jinja2 templates
# # FastAPI sẽ tìm kiếm các file HTML trong thư mục 'app/templates'
templates = Jinja2Templates(directory='app/templates')

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

def get_week_date_range(year: int, week: int) -> (datetime, datetime):
    """ Lấy ngày bắt đầu và kết thúc của một tuần trong năm. """
    d = date(year, 1, 1)
    d = d + timedelta(weeks=week - 1, days=-d.weekday())
    start_date = datetime.combine(d, datetime.min.time())
    end_date = start_date + timedelta(days=7)

    return start_date, end_date

@app.get('/')
async def read_root(request: Request):
    """ Endpoint chính, render trang dashboard. """
    return templates.TemplateResponse('dashboard.html', {'request': request})

# @app.get('/api/data')
# async def get_dashboard_data(
#     period: str = Query('day', enum=['day', 'week', 'month', 'year']),
#     store_id: Optional[int] = Query(None),
#     selected_date_str: Optional[str] = Query(None), # YYYY-MM-DD
#     year: Optional[int] = Query(None),
#     month: Optional[int] = Query(None),
#     week: Optional[int] = Query(None),
#     db: Session = Depends(get_db)
# ):
#     """ API cung cấp toàn bộ dữ liệu cho dashboard. """
#     now = datetime.now()

#     # Xác định khoảng thời gian hiện tại
#     if period == 'day':
#         current_day = datetime.strptime(selected_date_str, '%Y-%m-%d') if selected_date_str else now
#         start_date = current_day.replace(hour=0, minute=0, second=0)
#         end_date = current_day.replace(hour=23, minute=59, second=59)
#     # Các logic xác định start_date, end_date cho week, month, year tương tự...
#     # (Code chi tiết sẽ phức tạp, ở đây ta giả định đã có)

#     # Lấy dữ liệu thô từ CSDL
#     # Trong thực tế, bạn sẽ cần logic phức tạp hơn để xử lý các bộ lọc
#     # Ở đây, chúng ta sẽ mô phỏng việc lấy dữ liệu của 7 ngày gần nhất để tính toán
#     end_date = now
#     start_date = now - timedelta(days=7)

#     all_data = crud.get_crowd_data_in_range(db, start_date, end_date)

#     # Lọc theo cửa hàng nếu có
#     if store_id:
#         all_data = [d for d in all_data if d.storeid == store_id]

#     # --- Xử lý dữ liệu để tạo các số liệu ---
#     # Đây là phần logic Data Analysis chính
#     # Do phức tạp, phần này sẽ được mô phỏng. Trong thực tế, bạn sẽ
#     # dùng các thư viện như Pandas hoặc các câu lệnh SQL phức tạp.

#     # 1. Dữ liệu cho Line Chart (giả định theo ngày)
#     labels = [(end_date - timedelta(days=i)).strftime('%d/%m') for i in range(6, -1, -1)]
#     line_data = [d.in_num for d in all_data[:7]] if len(all_data) >= 7 else [150, 220, 300, 250, 400, 380, 500]

#     # 2. Dữ liệu cho Donut Chart
#     stores = crud.get_stores(db)
#     store_traffic = {store.name: 0 for store in stores}
#     for record in all_data:
#         store_name = next((s.name for s in stores if s.tid == record.storeid), 'Không rõ')
#         store_traffic[store_name] += record.in_num

#     donut_labels = list(store_traffic.keys())
#     donut_data = list(store_traffic.values())

#     # 3. Các số liệu (Metrics)
#     total_in = sum(d.in_num for d in all_data)
#     average_in = total_in / len(all_data) if all_data else 0

#     # 4. Lấy lỗi
#     error_logs = crud.get_error_logs(db)

#     return {
#         'line_chart_data': {'labels': labels, 'data': line_data},
#         'donut_chart_data': {'labels': donut_labels, 'data': donut_data},
#         'table_data': {'labels': labels, 'data': line_data},
#         'metrics': {
#             'total_in': total_in,
#             'average_in': average_in,
#             'peak_time': '19:00', # Giả định
#             'occupancy': total_in - sum(d.out_num for d in all_data),
#             'busiest_store': max(store_traffic, key=store_traffic.get) if store_traffic else '--',
#             'growth': 15.2 # Giả định
#         },
#         'error_logs': error_logs,
#         'stores': stores
#     }



@app.get("/api/data")
async def get_dashboard_data(
    period: str = Query("day", enum=["day", "week", "month", "year"]),
    store_id: Optional[str] = Query(None), # SỬA LỖI: Chấp nhận chuỗi thay vì số
    anomaly_threshold: float = Query(3.0), # THÊM: Tham số còn thiếu
    selected_date_str: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    week: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    """API cung cấp toàn bộ dữ liệu cho dashboard."""
    now = datetime.now()
    
    # --- Xác định khoảng thời gian hiện tại (current) và trước đó (previous) ---
    # Current period
    if period == "day":
        current_start_date = datetime.strptime(selected_date_str, "%Y-%m-%d") if selected_date_str else now
        current_start_date = current_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        current_end_date = current_start_date + timedelta(days=1)
        prev_start_date = current_start_date - timedelta(days=1)
        prev_end_date = current_start_date
    elif period == "week":
        year = year or now.year
        week = week or now.isocalendar()[1]
        current_start_date, current_end_date = get_week_date_range(year, week)
        prev_start_date, prev_end_date = get_week_date_range(year, week - 1) if week > 1 else get_week_date_range(year - 1, 52)
    elif period == "month":
        year = year or now.year
        month = month or now.month
        current_start_date = datetime(year, month, 1)
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        current_end_date = datetime(next_year, next_month, 1)
        prev_month = month - 1 if month > 1 else 12
        prev_year = year if month > 1 else year - 1
        prev_start_date = datetime(prev_year, prev_month, 1)
        prev_end_date = current_start_date
    else: # year
        year = year or now.year
        current_start_date = datetime(year, 1, 1)
        current_end_date = datetime(year + 1, 1, 1)
        prev_start_date = datetime(year - 1, 1, 1)
        prev_end_date = current_start_date

    # --- Lấy dữ liệu từ CSDL ---
    current_data_raw = crud.get_crowd_data_in_range(db, current_start_date, current_end_date)
    prev_data_raw = crud.get_crowd_data_in_range(db, prev_start_date, prev_end_date)
    
    # --- Xử lý bộ lọc cửa hàng (Store Filter) ---
    store_id_int = None
    if store_id and store_id.isdigit():
        store_id_int = int(store_id)
        current_data_raw = [d for d in current_data_raw if d.storeid == store_id_int]
        # prev_data_raw không cần lọc theo cửa hàng vì nó chỉ dùng để tính tăng trưởng tổng
    
    # --- Xử lý làm mịn dữ liệu (Anomaly Smoothing) ---
    if len(current_data_raw) > 2:
        avg = sum(d.in_num for d in current_data_raw) / len(current_data_raw)
        threshold = avg * anomaly_threshold
        for d in current_data_raw:
            if d.in_num > threshold:
                d.in_num = int(threshold)

    # --- Tổng hợp dữ liệu cho biểu đồ và bảng ---
    agg_data = defaultdict(int)
    if period == "day":
        for d in current_data_raw: agg_data[d.recordtime.hour] += d.in_num
        labels = [f"{h}:00" for h in range(24)]
        data = [agg_data.get(h, 0) for h in range(24)]
    elif period == "week":
        for d in current_data_raw: agg_data[d.recordtime.weekday()] += d.in_num
        day_names = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
        labels = day_names
        data = [agg_data.get(i, 0) for i in range(7)]
    elif period == "month":
        days_in_month = (current_end_date - current_start_date).days
        for d in current_data_raw: agg_data[d.recordtime.day] += d.in_num
        labels = [f"Ngày {i}" for i in range(1, days_in_month + 1)]
        data = [agg_data.get(i, 0) for i in range(1, days_in_month + 1)]
    else: # year
        for d in current_data_raw: agg_data[d.recordtime.month] += d.in_num
        labels = [f"Tháng {i}" for i in range(1, 13)]
        data = [agg_data.get(i, 0) for i in range(1, 13)]

    # --- Tính toán các số liệu (Metrics) ---
    total_in = sum(data)
    average_in = total_in / len([d for d in data if d > 0]) if any(d > 0 for d in data) else 0
    peak_value = max(data) if data else 0
    peak_time = labels[data.index(peak_value)] if peak_value > 0 else "--"
    
    total_out = sum(d.out_num for d in current_data_raw)
    occupancy = sum(d.in_num - d.out_num for d in current_data_raw)

    prev_total_in = sum(d.in_num for d in prev_data_raw)
    growth = ((total_in - prev_total_in) / prev_total_in) * 100 if prev_total_in > 0 else (100 if total_in > 0 else 0)

    # --- Dữ liệu cho Donut Chart ---
    stores = crud.get_stores(db)
    all_current_data = crud.get_crowd_data_in_range(db, current_start_date, current_end_date)
    store_traffic = defaultdict(int)
    for record in all_current_data:
        store_traffic[record.storeid] += record.in_num
    
    busiest_store_name = "--"
    if store_traffic:
        busiest_store_id = max(store_traffic, key=store_traffic.get)
        busiest_store_name = next((s.name for s in stores if s.tid == busiest_store_id), "Không rõ")

    donut_labels = [next((s.name for s in stores if s.tid == sid), "Không rõ") for sid in store_traffic.keys()]
    donut_data = list(store_traffic.values())

    # --- Chuẩn bị dữ liệu trả về ---
    time_range_display = f"{current_start_date.strftime('%d/%m/%Y')} - { (current_end_date - timedelta(seconds=1)).strftime('%d/%m/%Y')}"
    if period == "day":
        time_range_display = current_start_date.strftime('%A, %d/%m/%Y')

    return {
        "line_chart_data": {"labels": labels, "data": data},
        "donut_chart_data": {"labels": donut_labels, "data": donut_data},
        "table_data": {"labels": labels, "data": data},
        "metrics": {
            "total_in": total_in,
            "average_in": average_in,
            "peak_time": peak_time,
            "occupancy": occupancy,
            "busiest_store": busiest_store_name,
            "growth": growth
        },
        "error_logs": crud.get_error_logs(db),
        "stores": stores,
        "time_range_display": time_range_display
    }







# import uvicorn

# from typing import Optional, Dict, List, Any
# 

# import crud, models, schemas
# from database import engine, get_db

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
#         db.commit() # Commit stores để lấy tid
        
#         crowd_data_to_add = []
#         for i in range(30 * 24): # Dữ liệu 30 ngày
#             for store in stores_to_add:
#                 record_time = datetime.now() - timedelta(hours=i)
#                 in_num = 50 + (record_time.hour % 24) * 5 + (-1)**i * 10
#                 if record_time.weekday() >= 5: in_num *= 1.5 # Cuối tuần
#                 out_num = int(in_num * (0.8 + (-1)**i * 0.1))
#                 crowd_data_to_add.append(models.NumCrowd(
#                     recordtime=record_time, storeid=store.tid, in_num=int(in_num), out_num=int(out_num)
#                 ))
#         db.bulk_save_objects(crowd_data_to_add)
#         db.commit()
#     db.close()
    
#     uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
    
    
    
    
    
    
    
    
    
    
    
    
    
    



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

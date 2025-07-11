# from sqlalchemy import func, text
# from typing import List, Optional


# # Import các thành phần cần thiết từ các file khác trong project
# # Giả định bạn đã có file db.py để quản lý session và models.py định nghĩa các table
# from .core.db import get_db 
# from . import schemas

# # Khởi tạo router
# router = APIRouter(
#     prefix="/api",
#     tags=["dashboard"],
#     responses={404: {"description": "Not found"}},
# )

# # ============================================
# # API Endpoint chính để lấy dữ liệu cho Dashboard
# # ============================================
# @router.get("/dashboard-data", response_model=schemas.DashboardDataResponse)
# def get_dashboard_data(
#     # --- Tham số lọc ---
#     period_type: str = Query("year", description="Loại chu kỳ: 'year', 'month', 'week', 'day'"),
#     year: int = Query(default=datetime.now().year, description="Năm cần xem"),
#     month: Optional[int] = Query(default=None, description="Tháng cần xem (1-12)"),
#     week: Optional[int] = Query(default=None, description="Tuần cần xem (1-53)"),
#     day: Optional[str] = Query(default=None, description="Ngày cần xem (YYYY-MM-DD)"),
#     store_id: Optional[int] = Query(default=None, description="Lọc theo ID của cửa hàng (store.tid)"),
#     # --- Dependency Injection ---
#     db: Session = Depends(get_db)
# ):
#     """
#     Endpoint tổng hợp, trả về tất cả dữ liệu cần thiết để hiển thị trên dashboard.
#     Bao gồm dữ liệu cho biểu đồ đường, biểu đồ donut, bảng tổng hợp và logs lỗi.
#     """
    
#     # --- 1. Xác định khoảng thời gian bắt đầu và kết thúc dựa trên tham số ---
#     start_date, end_date = get_date_range(period_type, year, month, week, day)

#     # --- 2. Xây dựng câu query cơ bản để lấy dữ liệu num_crowd ---
#     base_query = db.query(models.NumCrowd).filter(
#         models.NumCrowd.recordtime >= start_date,
#         models.NumCrowd.recordtime < end_date
#     )
#     if store_id:
#         base_query = base_query.filter(models.NumCrowd.storeid == store_id)

#     # --- 3. Lấy dữ liệu cho biểu đồ Time Series (Biểu đồ đường) ---
#     time_series_data = get_time_series_data(base_query, period_type, db)
    
#     # --- 4. Lấy dữ liệu cho bảng tổng hợp và tính toán chênh lệch % ---
#     aggregated_table_data = calculate_percentage_change(time_series_data)

#     # --- 5. Lấy dữ liệu cho biểu đồ Donut (So sánh các cửa hàng) ---
#     # Query này sẽ chạy trên cùng khoảng thời gian nhưng group by cửa hàng
#     store_comparison_query = db.query(
#         models.Store.name.label("store_name"),
#         func.sum(models.NumCrowd.in_num).label("total_in")
#     ).join(models.Store, models.NumCrowd.storeid == models.Store.tid).filter(
#         models.NumCrowd.recordtime >= start_date,
#         models.NumCrowd.recordtime < end_date
#     ).group_by(models.Store.name).order_by(func.sum(models.NumCrowd.in_num).desc())
    
#     store_comparison_data = store_comparison_query.all()

#     # --- 6. Lấy danh sách các cửa hàng để hiển thị trên bộ lọc ---
#     stores = db.query(models.Store).order_by(models.Store.name).all()

#     # --- 7. Lấy các log lỗi gần đây (ví dụ: 50 logs mới nhất) ---
#     error_logs = db.query(models.ErrLog).order_by(models.ErrLog.LogTime.desc()).limit(50).all()

#     # --- 8. Trả về dữ liệu theo schema đã định nghĩa ---
#     return {
#         "time_series_data": time_series_data,
#         "store_comparison_data": store_comparison_data,
#         "aggregated_table_data": aggregated_table_data,
#         "error_logs": error_logs,
#         "stores": stores
#     }

# # ============================================
# # API Endpoint để xuất dữ liệu chi tiết
# # ============================================
# @router.get("/export-data", response_model=List[schemas.DetailDataRow])
# def export_detailed_data(
#     # --- Tham số tương tự như endpoint chính ---
#     period_type: str = Query("year", description="Loại chu kỳ: 'year', 'month', 'week', 'day'"),
#     year: int = Query(default=datetime.now().year),
#     month: Optional[int] = Query(default=None),
#     week: Optional[int] = Query(default=None),
#     day: Optional[str] = Query(default=None),
#     store_id: Optional[int] = Query(default=None),
#     db: Session = Depends(get_db)
# ):
#     """
#     Endpoint này trả về dữ liệu chi tiết (chưa tổng hợp) để người dùng có thể tải về.
#     """
#     start_date, end_date = get_date_range(period_type, year, month, week, day)

#     query = db.query(
#         models.NumCrowd.recordtime,
#         models.NumCrowd.in_num,
#         models.NumCrowd.out_num,
#         models.NumCrowd.position,
#         models.Store.name.label("store_name")
#     ).join(models.Store, models.NumCrowd.storeid == models.Store.tid).filter(
#         models.NumCrowd.recordtime >= start_date,
#         models.NumCrowd.recordtime < end_date
#     )

#     if store_id:
#         query = query.filter(models.NumCrowd.storeid == store_id)
        
#     return query.order_by(models.NumCrowd.recordtime).all()


# # ============================================
# # Các hàm tiện ích (Helper Functions)
# # ============================================

# def get_date_range(period_type, year, month, week, day_str):
#     """Xác định ngày bắt đầu và kết thúc dựa trên các tham số đầu vào."""
#     if period_type == 'day' and day_str:
#         start_date = datetime.strptime(day_str, "%Y-%m-%d")
#         end_date = start_date + timedelta(days=1)
#     elif period_type == 'week' and week:
#         # Tuần bắt đầu từ thứ 2
#         start_date = datetime.strptime(f'{year}-W{week}-1', "%Y-W%W-%w")
#         end_date = start_date + timedelta(days=7)
#     elif period_type == 'month' and month:
#         start_date = datetime(year, month, 1)
#         next_month = start_date.replace(day=28) + timedelta(days=4)
#         end_date = next_month - timedelta(days=next_month.day - 1)
#     else: # Mặc định là 'year'
#         start_date = datetime(year, 1, 1)
#         end_date = datetime(year + 1, 1, 1)
        
#     return start_date, end_date

# def get_time_series_data(base_query, period_type, db: Session):
#     """Thực hiện aggregation dữ liệu theo chu kỳ thời gian."""
    
#     # NOTE: Đây là nơi bạn có thể áp dụng logic xử lý outlier/nhiễu
#     # Ví dụ: loại bỏ các giá trị in_num > ngưỡng nào đó.
#     # base_query = base_query.filter(models.NumCrowd.in_num < 1000) # Ví dụ đơn giản
    
#     # Xác định định dạng group by cho SQL
#     if period_type == 'day':
#         group_format = "%Y-%m-%d"
#         group_clause = func.strftime(group_format, models.NumCrowd.recordtime)
#     elif period_type == 'week':
#         group_format = "%Y-W%W" # Năm và số tuần
#         group_clause = func.strftime(group_format, models.NumCrowd.recordtime)
#     elif period_type == 'month':
#         group_format = "%Y-%m"
#         group_clause = func.strftime(group_format, models.NumCrowd.recordtime)
#     else: # year
#         group_format = "%Y-%m" # Group theo tháng trong năm
#         group_clause = func.strftime(group_format, models.NumCrowd.recordtime)

#     # Câu query tổng hợp
#     time_series_query = base_query.with_entities(
#         group_clause.label("period"),
#         func.sum(models.NumCrowd.in_num).label("in_count")
#     ).group_by("period").order_by("period")
    
#     return time_series_query.all()

# def calculate_percentage_change(data: List[schemas.TimeSeriesDataPoint]) -> List[schemas.AggregatedTableRow]:
#     """Tính toán phần trăm thay đổi so với dòng trước đó."""
#     table_rows = []
#     for i, row in enumerate(data):
#         percentage_change = None
#         if i > 0 and data[i-1].in_count > 0:
#             change = ((row.in_count - data[i-1].in_count) / data[i-1].in_count) * 100
#             percentage_change = round(change, 2)
        
#         table_rows.append(
#             schemas.AggregatedTableRow(
#                 period=row.period,
#                 total_in=row.in_count,
#                 percentage_change=percentage_change
#             )
#         )
#     return table_rows










# import pandas as pd
# from datetime import datetime, timedelta, date
# from fastapi import APIRouter, Request, Query, Depends, HTTPException
# from fastapi.responses import HTMLResponse, FileResponse
# from fastapi.templating import Jinja2Templates
# from sqlalchemy.orm import Session
# from typing import List

# from .core.db import get_db, execute_query_as_dataframe
# from .schemas import ErrorLog, Store, FullStatsResponse, StatsResponse, DonutChartData, TableDataItem

# # Khởi tạo router và templates
# router = APIRouter()
# templates = Jinja2Templates(directory='app/templates')

# def get_week_details(year: int, week_num: int) -> (date, date):
#     """Lấy ngày bắt đầu và kết thúc của một tuần trong năm."""
#     first_day_of_year = date(year, 1, 1)
#     # Ngày đầu tiên của năm là thứ mấy (Monday=0, Sunday=6)
#     first_day_weekday = first_day_of_year.weekday()
#     # Tìm ngày thứ Hai đầu tiên của năm
#     if first_day_weekday < 4: # Nếu ngày 1/1 là Mon, Tue, Wed, Thu -> nó thuộc tuần 1
#         start_of_week1 = first_day_of_year - timedelta(days=first_day_weekday)
#     else:
#         start_of_week1 = first_day_of_year + timedelta(days=7 - first_day_weekday)
    
#     start_of_week = start_of_week1 + timedelta(weeks=week_num - 1)
#     end_of_week = start_of_week + timedelta(days=6)
#     return start_of_week, end_of_week

# def smooth_data_by_std(df: pd.DataFrame, column: str, std_dev_threshold: float = 2.5) -> pd.DataFrame:
#     """
#     Hàm xử lý dữ liệu bất thường bằng cách thay thế các giá trị ngoại lai
#     bằng giá trị trung bình của các điểm lân cận.
#     """
#     if df.empty or column not in df.columns or len(df) < 3:
#         return df

#     df_copy = df.copy()
#     df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce').fillna(0)
    
#     mean = df_copy[column].mean()
#     std_dev = df_copy[column].std()
    
#     if std_dev == 0:
#         return df # Không có biến động

#     # Xác định ngưỡng trên và dưới
#     upper_bound = mean + std_dev * std_dev_threshold
    
#     # Thay thế các giá trị vượt ngưỡng bằng giá trị trung bình (hoặc một giá trị hợp lý hơn)
#     # Ở đây ta thay bằng chính ngưỡng đó để tránh làm mất đi xu hướng tăng đột biến
#     df_copy.loc[df_copy[column] > upper_bound, column] = upper_bound
    
#     return df_copy

# @router.get('/', response_class=HTMLResponse)
# async def read_root(request: Request):
#     """
#     Endpoint chính, render trang dashboard.
#     """
#     # Truyền các dữ liệu cần thiết cho template, ví dụ như danh sách cửa hàng
#     return templates.TemplateResponse('dashboard.html', {'request': request})

# @router.get('/api/stores', response_model=List[Store])
# async def get_stores(db: Session = Depends(get_db)):
#     """
#     API endpoint để lấy danh sách tất cả các cửa hàng.
#     """
#     query = "SELECT tid, name FROM dbo.store ORDER BY name;"
#     df = execute_query_as_dataframe(query, db)
#     if df.empty:
#         # Trả về danh sách rỗng thay vì lỗi 404 để frontend xử lý dễ hơn
#         return []
#     return df.to_dict(orient='records')

# @router.get('/api/stats', response_model=FullStatsResponse)
# async def get_stats(
#     period: str = Query('year', enum=['day', 'week', 'month', 'year']),
#     store_id: int = Query(0, description="0 for all stores"),
#     year: int = Query(None),
#     month: int = Query(None),
#     week: int = Query(None),
#     day: str = Query(None, description="Format YYYY-MM-DD"),
#     smooth_ratio: float = Query(3.0, ge=1.0, le=10.0, description="Tỷ lệ xử lý đột biến"),
#     db: Session = Depends(get_db)
# ):
#     """
#     API endpoint chính để lấy dữ liệu thống kê.
#     """
#     now = datetime.now()
    
#     # --- Xác định khoảng thời gian truy vấn (start_date, end_date) ---
#     if period == 'day':
#         query_date = datetime.strptime(day, '%Y-%m-%d').date() if day else now.date()
#         start_date = datetime.combine(query_date, datetime.min.time())
#         end_date = datetime.combine(query_date, datetime.max.time())
#         group_by_clause = "DATEPART(hour, recordtime)"
#         label_format = "{h}:00"
#         time_range_display = query_date.strftime('%d/%m/%Y')
#     elif period == 'week':
#         query_year = year if year else now.year
#         query_week = week if week else now.isocalendar()[1]
#         start_date, end_date = get_week_details(query_year, query_week)
#         start_date = datetime.combine(start_date, datetime.min.time())
#         end_date = datetime.combine(end_date, datetime.max.time())
#         group_by_clause = "CONVERT(date, recordtime)"
#         label_format = "weekday" # Sẽ xử lý trong pandas
#         time_range_display = f"{start_date.strftime('%d/%m')} - {end_date.strftime('%d/%m/%Y')}"
#     elif period == 'month':
#         query_year = year if year else now.year
#         query_month = month if month else now.month
#         start_date = datetime(query_year, query_month, 1)
#         next_month = start_date.replace(day=28) + timedelta(days=4)
#         end_date = next_month - timedelta(days=next_month.day)
#         end_date = datetime.combine(end_date, datetime.max.time())
#         group_by_clause = "DATEPART(day, recordtime)"
#         label_format = "Ngày {d}"
#         time_range_display = f"Tháng {query_month}/{query_year}"
#     else: # year
#         query_year = year if year else now.year
#         start_date = datetime(query_year, 1, 1)
#         end_date = datetime(query_year, 12, 31, 23, 59, 59)
#         group_by_clause = "DATEPART(month, recordtime)"
#         label_format = "Tháng {m}"
#         time_range_display = f"Năm {query_year}"

#     # --- Xây dựng câu lệnh SQL ---
#     params = {'start_date': start_date, 'end_date': end_date}
#     store_filter_clause = ""
#     if store_id != 0:
#         store_filter_clause = "AND storeid = :store_id"
#         params['store_id'] = store_id

#     query = f"""
#         SELECT
#             {group_by_clause} as group_key,
#             SUM(CAST(in_num AS BIGINT)) as value
#         FROM dbo.num_crowd
#         WHERE recordtime BETWEEN :start_date AND :end_date
#         {store_filter_clause}
#         GROUP BY {group_by_clause}
#         ORDER BY group_key;
#     """
    
#     df = execute_query_as_dataframe(query, db, params=params)

#     # --- Xử lý dữ liệu với Pandas ---
#     if not df.empty:
#         df = smooth_data_by_std(df, 'value', smooth_ratio)

#     # --- Chuẩn bị dữ liệu cho Line Chart ---
#     labels, data = [], []
#     if period == 'day':
#         df.set_index('group_key', inplace=True)
#         for h in range(24):
#             labels.append(label_format.format(h=h))
#             data.append(df.loc[h, 'value'] if h in df.index else 0)
#     elif period == 'week':
#         df['group_key'] = pd.to_datetime(df['group_key'])
#         df.set_index('group_key', inplace=True)
#         days_of_week_vn = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật']
#         for i in range(7):
#             current_day = start_date.date() + timedelta(days=i)
#             labels.append(days_of_week_vn[i])
#             data.append(df.loc[current_day, 'value'] if current_day in df.index else 0)
#     elif period == 'month':
#         df.set_index('group_key', inplace=True)
#         days_in_month = (end_date.date() - start_date.date()).days + 1
#         for d in range(1, days_in_month + 1):
#             labels.append(label_format.format(d=d))
#             data.append(df.loc[d, 'value'] if d in df.index else 0)
#     else: # year
#         df.set_index('group_key', inplace=True)
#         for m in range(1, 13):
#             labels.append(label_format.format(m=m))
#             data.append(df.loc[m, 'value'] if m in df.index else 0)
    
#     data = [round(x, 2) for x in data]

#     # --- Tính toán các chỉ số ---
#     total_in = sum(data)
#     average_in = total_in / len(data) if data else 0
#     max_in = max(data) if data else 0
    
#     # --- Tính toán tăng trưởng (so với kỳ trước) ---
#     # (Phần này có thể phức tạp, tạm thời để là 0)
#     growth = 0.0

#     line_chart_response = StatsResponse(
#         period=period, labels=labels, data=data,
#         total_in=total_in, average_in=average_in, max_in=max_in,
#         growth=growth, time_range=time_range_display
#     )

#     # --- Chuẩn bị dữ liệu cho Donut Chart (Tỷ trọng theo cửa) ---
#     donut_query = """
#         SELECT s.name, SUM(CAST(nc.in_num AS BIGINT)) as value
#         FROM dbo.num_crowd nc
#         JOIN dbo.store s ON nc.storeid = s.tid
#         WHERE nc.recordtime BETWEEN :start_date AND :end_date
#         GROUP BY s.name
#         HAVING SUM(CAST(nc.in_num AS BIGINT)) > 0
#         ORDER BY value DESC;
#     """
#     donut_df = execute_query_as_dataframe(donut_query, db, params={'start_date': start_date, 'end_date': end_date})
#     donut_chart_data = DonutChartData(
#         labels=donut_df['name'].tolist() if not donut_df.empty else [],
#         data=donut_df['value'].tolist() if not donut_df.empty else []
#     )

#     # --- Chuẩn bị dữ liệu cho Table ---
#     table_data = []
#     for i, label in enumerate(labels):
#         current_value = data[i]
#         prev_value = data[i-1] if i > 0 else 0
#         diff = ((current_value - prev_value) / prev_value) * 100 if prev_value > 0 else (100 if current_value > 0 else 0)
#         table_data.append(TableDataItem(label=label, value=current_value, difference=round(diff, 2)))

#     return FullStatsResponse(
#         line_chart=line_chart_response,
#         donut_chart=donut_chart_data,
#         table_data=table_data
#     )

# @router.get('/api/errors', response_model=List[ErrorLog])
# async def get_error_logs(
#     limit: int = Query(10, ge=1, le=50),
#     db: Session = Depends(get_db)
# ):
#     """
#     API endpoint để lấy các log lỗi mới nhất.
#     """
#     query = """
#         SELECT TOP (:limit) ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage
#         FROM dbo.ErrLog
#         ORDER BY LogTime DESC;
#     """
#     df = execute_query_as_dataframe(query, db, params={'limit': limit})
#     if df.empty:
#         return []
#     return df.to_dict(orient='records')

# @router.get("/api/download", response_class=FileResponse)
# async def download_data(
#     period: str = Query('year', enum=['day', 'week', 'month', 'year']),
#     store_id: int = Query(0),
#     year: int = Query(None),
#     month: int = Query(None),
#     week: int = Query(None),
#     day: str = Query(None),
#     db: Session = Depends(get_db)
# ):
#     """
#     Endpoint để tải dữ liệu chi tiết dạng CSV.
#     Logic truy vấn tương tự get_stats nhưng không group by.
#     """
#     # (Tương tự get_stats để xác định start_date, end_date)
#     # ...
#     # Sau đó, query dữ liệu chi tiết
#     # query = "SELECT recordtime, in_num, out_num, position, storeid FROM dbo.num_crowd WHERE ..."
#     # df = execute_query_as_dataframe(query, db, params=...)
#     # file_path = "temp_data.csv"
#     # df.to_csv(file_path, index=False)
#     # return FileResponse(path=file_path, media_type='text/csv', filename=f'data_{period}.csv')
    
#     # Tạm thời trả về lỗi vì logic cần hoàn thiện
#     raise HTTPException(status_code=501, detail="Tính năng đang được phát triển.")


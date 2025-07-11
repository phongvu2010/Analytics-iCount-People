import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional

from .core.db import get_db, execute_query_as_dataframe
from .schemas import StatsResponse, ErrorLog, Store, Metrics, StatData, DonutData

router = APIRouter()
templates = Jinja2Templates(directory='app/templates')

def smooth_data(df: pd.DataFrame, column: str = 'value', std_dev_threshold: float = 2.5) -> pd.DataFrame:
    # ... (Hàm này giữ nguyên, không thay đổi)
    if df.empty or column not in df.columns:
        return df
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df.dropna(subset=[column], inplace=True)
    if df.empty:
        return df
    mean = df[column].mean()
    std_dev = df[column].std()
    df_filtered = df[np.abs(df[column] - mean) <= std_dev * std_dev_threshold]
    return df_filtered

@router.get('/', response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse('dashboard.html', {'request': request})

# --- Endpoint MỚI để lấy danh sách cửa hàng ---
@router.get('/api/stores', response_model=list[Store])
async def get_stores(db: Session = Depends(get_db)):
    """
    API endpoint để lấy danh sách tất cả cửa hàng.
    """
    query = "SELECT tid, name FROM dbo.store ORDER BY name;"
    df = execute_query_as_dataframe(query, db)
    return df.to_dict(orient='records')

@router.get('/api/errors', response_model=list[ErrorLog])
async def get_error_logs(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    # ... (Endpoint này giữ nguyên, không thay đổi)
    query = f"SELECT TOP (?) ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage FROM dbo.ErrLog ORDER BY LogTime DESC;"
    df = execute_query_as_dataframe(query, db, params=(limit,))
    if df.empty:
        return []
    return df.to_dict(orient='records')

# --- Endpoint /api/stats được VIẾT LẠI HOÀN TOÀN ---
@router.get('/api/stats', response_model=StatsResponse)
async def get_stats(
    period: str = Query('day', enum=['day', 'week', 'month', 'year']),
    store_id: int = Query(0), # 0 nghĩa là tất cả cửa hàng
    smooth_threshold: Optional[float] = Query(2.5, ge=1.0, le=10.0),
    target_date_str: Optional[str] = Query(None), # YYYY-MM-DD
    db: Session = Depends(get_db)
):
    """
    API endpoint đa năng để lấy dữ liệu thống kê.
    - period: 'day', 'week', 'month', 'year'
    - store_id: ID của cửa hàng, 0 = tất cả
    - target_date_str: Ngày mục tiêu để tính toán (YYYY-MM-DD). Mặc định là hôm nay.
    """
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date() if target_date_str else date.today()

    # 1. Xác định khoảng thời gian (start_date, end_date) và chu kỳ trước đó
    if period == 'day':
        start_date = target_date
        end_date = start_date
        prev_start_date = start_date - timedelta(days=1)
        prev_end_date = prev_start_date
        group_by_clause = 'DATEPART(hour, recordtime)'
        label_clause = "CAST(DATEPART(hour, recordtime) AS VARCHAR)"
        time_range_display = start_date.strftime('%d/%m/%Y')
    elif period == 'week':
        start_date = target_date - timedelta(days=target_date.weekday())
        end_date = start_date + timedelta(days=6)
        prev_start_date = start_date - timedelta(days=7)
        prev_end_date = end_date - timedelta(days=7)
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = 'FORMAT(CONVERT(date, recordtime), \'dd/MM\')'
        time_range_display = f"{start_date.strftime('%d/%m')} - {end_date.strftime('%d/%m/%Y')}"
    elif period == 'month':
        start_date = target_date.replace(day=1)
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day)
        prev_end_date = start_date - timedelta(days=1)
        prev_start_date = prev_end_date.replace(day=1)
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = 'FORMAT(CONVERT(date, recordtime), \'dd/MM\')'
        time_range_display = f"Tháng {start_date.month}, {start_date.year}"
    else: # year
        start_date = target_date.replace(month=1, day=1)
        end_date = target_date.replace(month=12, day=31)
        prev_start_date = start_date.replace(year=start_date.year - 1)
        prev_end_date = end_date.replace(year=end_date.year - 1)
        group_by_clause = 'FORMAT(recordtime, \'yyyy-MM\')'
        label_clause = 'FORMAT(recordtime, \'yyyy-MM\')'
        time_range_display = f"Năm {start_date.year}"

    # 2. Xây dựng câu query chính
    params = [start_date, end_date]
    store_filter = ""
    if store_id != 0:
        store_filter = "AND storeid = ?"
        params.append(store_id)

    query = f"""
        SELECT {label_clause} as label, SUM(in_num) as value
        FROM dbo.num_crowd
        WHERE CONVERT(date, recordtime) BETWEEN ? AND ? {store_filter}
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause};
    """
    df = execute_query_as_dataframe(query, db, params=tuple(params))
    
    # 3. Xử lý dữ liệu
    df_smoothed = smooth_data(df.copy(), column='value', std_dev_threshold=smooth_threshold)
    table_data = df_smoothed.to_dict(orient='records')

    # 4. Lấy dữ liệu cho Donut chart (luôn lấy trong khoảng thời gian đã chọn)
    donut_query = f"""
        SELECT T2.name as label, SUM(T1.in_num) as value
        FROM dbo.num_crowd T1 JOIN dbo.store T2 ON T1.storeid = T2.tid
        WHERE CONVERT(date, T1.recordtime) BETWEEN ? AND ?
        GROUP BY T2.name
        ORDER BY value DESC;
    """
    donut_df = execute_query_as_dataframe(donut_query, db, params=(start_date, end_date))
    donut_data = donut_df.to_dict(orient='records')
    
    # 5. Lấy dữ liệu kỳ trước để tính tăng trưởng
    prev_params = [prev_start_date, prev_end_date]
    if store_id != 0:
        prev_params.append(store_id)
        
    prev_query = f"""
        SELECT SUM(in_num) as total
        FROM dbo.num_crowd
        WHERE CONVERT(date, recordtime) BETWEEN ? AND ? {store_filter};
    """
    prev_df = execute_query_as_dataframe(prev_query, db, params=tuple(prev_params))
    prev_total_in = prev_df['total'].sum()

    # 6. Tính toán các chỉ số
    total_in = df_smoothed['value'].sum()
    max_in = df_smoothed['value'].max() if not df_smoothed.empty else 0
    average_in = df_smoothed['value'].mean() if not df_smoothed.empty else 0
    growth = ((total_in - prev_total_in) / prev_total_in) * 100 if prev_total_in > 0 else 100 if total_in > 0 else 0

    metrics = Metrics(
        total_in=total_in,
        average_in=average_in,
        max_in=max_in,
        growth=growth
    )

    # 7. Trả về response hoàn chỉnh
    return StatsResponse(
        period=period,
        time_range_display=time_range_display,
        metrics=metrics,
        chart_data=table_data, # Dữ liệu chart và table giống nhau
        donut_data=donut_data,
        table_data=table_data
    )





# import pandas as pd
# from fastapi import APIRouter, Depends, Query
# from sqlalchemy.orm import Session
# from datetime import datetime, date, timedelta
# from typing import List

# from . import schemas
# from .core.db import get_db, execute_query_as_dataframe

# # Khởi tạo router
# router = APIRouter(prefix='/api/v1', tags=['Dashboard Data'])

# # =============================================
# # API Endpoint: Lấy danh sách cửa hàng
# # =============================================
# @router.get('/stores', response_model = List[schemas.Store])
# def get_stores(db: Session = Depends(get_db)):
#     """ Cung cấp danh sách các cửa hàng để hiển thị trên bộ lọc dropdown. """
#     query = "SELECT tid, name FROM dbo.store ORDER BY name;"
#     df = execute_query_as_dataframe(query, db)

#     return df.to_dict(orient = 'records')

# # =============================================
# # API Endpoint: Lấy danh sách log lỗi
# # =============================================
# @router.get('/error-logs', response_model = List[schemas.ErrorLog])
# def get_error_logs(db: Session = Depends(get_db)):
#     """
#     Cung cấp danh sách các cảnh báo lỗi mới nhất để hiển thị trong chuông thông báo.
#     Join với bảng store để lấy tên cửa hàng.
#     """
#     query = """
#         SELECT TOP 15
#             e.ID,
#             e.storeid,
#             e.LogTime,
#             e.ErrorMessage,
#             s.name as store_name
#         FROM dbo.ErrLog e
#         LEFT JOIN dbo.store s ON e.storeid = s.tid
#         ORDER BY e.LogTime DESC;
#     """
#     df = execute_query_as_dataframe(query, db)
#     # Chuyển đổi NaN (nếu có) thành None để tương thích Pydantic
#     df = df.where(pd.notnull(df), None)

#     return df.to_dict(orient = 'records')

# # =============================================
# # API Endpoint: Lấy dữ liệu thống kê chính
# # =============================================
# @router.get('/traffic-data', response_model = schemas.TrafficDataResponse)
# def get_traffic_data(
#     period: str = Query('year', enum=['day', 'week', 'month', 'year']),
#     store_id: str = Query('all'),
#     selected_date: date = Query(None),
#     year: int = Query(None),
#     month: int = Query(None),
#     week: int = Query(None),
#     anomaly_threshold: float = Query(3.0, ge=1.5, le=10.0),
#     db: Session = Depends(get_db)
# ):
#     """ Endpoint chính, xử lý logic tính toán và trả về toàn bộ dữ liệu cho dashboard. """
#     # --- 1. Xác định khoảng thời gian (hiện tại và quá khứ) ---
#     now = datetime.now()
#     if not year: year = now.year
#     if not month: month = now.month
#     if not selected_date: selected_date = now.date()

#     # Hàm helper để lấy khoảng thời gian
#     def get_date_range(p, y, m, w, d):
#         if p == 'day':
#             start = datetime.combine(d, datetime.min.time())
#             end = datetime.combine(d, datetime.max.time())
#             prev_start = start - timedelta(days=1)
#             prev_end = end - timedelta(days=1)
#             range_display = d.strftime('%A, %d/%m/%Y')
#         elif p == 'week':
#             # Monday is 0 and Sunday is 6
#             start_of_year = date(y, 1, 1)
#             start_of_week_date = start_of_year + timedelta(days=(w-1)*7 - start_of_year.weekday())
#             start = datetime.combine(start_of_week_date, datetime.min.time())
#             end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
#             prev_start = start - timedelta(weeks=1)
#             prev_end = end - timedelta(weeks=1)
#             range_display = f"{start.strftime('%d/%m')} - {end.strftime('%d/%m/%Y')}"
#         elif p == 'month':
#             start = datetime(y, m, 1)
#             # Tìm ngày cuối cùng của tháng
#             next_month = start.replace(day=28) + timedelta(days=4)
#             end_of_month = next_month - timedelta(days=next_month.day)
#             end = end_of_month.replace(hour=23, minute=59, second=59)

#             prev_month_date = start - timedelta(days=1)
#             prev_start = prev_month_date.replace(day=1)
#             prev_end = start - timedelta(seconds=1)
#             range_display = f"Tháng {m}, {y}"
#         else: # year
#             start = datetime(y, 1, 1)
#             end = datetime(y, 12, 31, 23, 59, 59)
#             prev_start = datetime(y - 1, 1, 1)
#             prev_end = datetime(y - 1, 12, 31, 23, 59, 59)
#             range_display = f"Năm {y}"
#         return start, end, prev_start, prev_end, range_display

#     start_date, end_date, prev_start_date, prev_end_date, time_range_display = get_date_range(period, year, month, week, selected_date)

#     # --- 2. Hàm xử lý dữ liệu ---
#     def process_period_data(start, end, p, s_id):
#         # Xây dựng câu lệnh SQL
#         sql_query = "SELECT recordtime, in_num, storeid FROM dbo.num_crowd WHERE recordtime BETWEEN :start AND :end"
#         params = {'start': start, 'end': end}
#         if s_id != 'all':
#             sql_query += " AND storeid = :store_id"
#             params['store_id'] = int(s_id)

#         df = execute_query_as_dataframe(sql_query, db, params=params)
#         if df.empty:
#             return pd.DataFrame(columns = ['label', 'value']), pd.DataFrame(columns = ['label', 'value'])

#         # Xử lý đột biến (anomaly)
#         if len(df) > 2:
#             average = df['in_num'].mean()
#             threshold = average * anomaly_threshold
#             df['in_num'] = df['in_num'].apply(lambda x: min(x, threshold))

#         # Group by cho biểu đồ chính (main chart)
#         df['recordtime'] = pd.to_datetime(df['recordtime'])
#         if p == 'day':
#             grouper = df['recordtime'].dt.hour
#             labels = [f"{i}:00" for i in range(24)]
#         elif p == 'week':
#             grouper = df['recordtime'].dt.dayofweek # Monday=0, Sunday=6
#             labels = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật']
#         elif p == 'month':
#             grouper = df['recordtime'].dt.day
#             days_in_month = pd.Period(f'{start.year}-{start.month}').days_in_month
#             labels = [f"Ngày {i}" for i in range(1, days_in_month + 1)]
#         else: # year
#             grouper = df['recordtime'].dt.month
#             labels = [f"Tháng {i}" for i in range(1, 13)]

#         main_chart_df = df.groupby(grouper)['in_num'].sum().round().astype(int)
#         main_chart_df = main_chart_df.reindex(range(len(labels)), fill_value=0)
#         main_chart_df.index = labels

#         # Group by cho biểu đồ tròn (pie chart)
#         pie_chart_df = df.groupby('storeid')['in_num'].sum().round().astype(int)

#         return main_chart_df, pie_chart_df

#     # --- 3. Lấy và xử lý dữ liệu cho kỳ hiện tại và quá khứ ---
#     current_main_data, current_pie_data = process_period_data(start_date, end_date, period, store_id)
#     prev_main_data, _ = process_period_data(prev_start_date, prev_end_date, period, store_id)

#     # --- 4. Tính toán các chỉ số (metrics) ---
#     total_in = int(current_main_data.sum())
#     max_in = int(current_main_data.max()) if not current_main_data.empty else 0
#     average_in = float(current_main_data.mean()) if not current_main_data.empty else 0.0

#     prev_total_in = int(prev_main_data.sum())
#     growth_percentage = 0.0
#     if prev_total_in > 0:
#         growth_percentage = ((total_in - prev_total_in) / prev_total_in) * 100
#     elif total_in > 0:
#         growth_percentage = 100.0 # Tăng vô hạn nếu kỳ trước là 0

#     growth_status = 'stable'
#     if growth_percentage > 0.5: growth_status = 'increase'
#     if growth_percentage < -0.5: growth_status = 'decrease'

#     metrics = schemas.TrafficMetrics(
#         total_in=total_in,
#         average_in=average_in,
#         max_in=max_in,
#         growth=schemas.GrowthData(percentage=growth_percentage, status=growth_status)
#     )

#     # --- 5. Định dạng dữ liệu cho response ---
#     # Dữ liệu biểu đồ tròn
#     store_names_df = get_stores(db)
#     store_name_map = {s['tid']: s['name'] for s in store_names_df}
#     pie_chart_data = [
#         schemas.PieChartDataPoint(label=store_name_map.get(storeid, f"ID {storeid}"), value=value)
#         for storeid, value in current_pie_data.items()
#     ]

#     # Dữ liệu biểu đồ chính và bảng
#     main_chart_data = [
#         schemas.DataPoint(label=str(index), value=value)
#         for index, value in current_main_data.items()
#     ]

#     return schemas.TrafficDataResponse(
#         metrics = metrics,
#         main_chart_data = main_chart_data,
#         pie_chart_data = pie_chart_data,
#         table_data = main_chart_data, # Dữ liệu bảng giống biểu đồ chính
#         time_range_display = time_range_display
#     )

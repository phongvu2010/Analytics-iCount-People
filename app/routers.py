from fastapi import APIRouter, Query
from typing import List, Optional

from . import services, schemas


router = APIRouter()

# # Endpoint để phục vụ trang dashboard chính
# @router.get("/", response_class=HTMLResponse)
# async def read_root(request: Request):
#     return templates.TemplateResponse("dashboard.html", {"request": request})

# Endpoint chính để lấy dữ liệu thống kê
@router.get("/dashboard-data", response_model=Optional[schemas.DashboardData])
async def get_dashboard_data(
    period: str = Query("day", description="Khoảng thời gian: day, week, month, year"),
    store_id: str = Query("all", description="ID của cửa hàng hoặc 'all'"),
    start_date: str = Query(..., description="Ngày bắt đầu YYYY-MM-DD"),
    end_date: str = Query(..., description="Ngày kết thúc YYYY-MM-DD")
):
    data = services.get_dashboard_data(period, store_id, start_date, end_date)
    return data

# Endpoint lấy danh sách lỗi
@router.get("/error-logs", response_model=List[schemas.ErrorLog])
async def get_error_logs():
    return services.get_error_logs()

# Endpoint lấy danh sách cửa hàng
@router.get("/stores", response_model=List[schemas.Store])
async def get_stores():
    return services.get_all_stores()
















# from datetime import date, timedelta
# from fastapi import APIRouter, Query, HTTPException
# from typing import List, Optional

# from . import schemas, services

# router = APIRouter()

# def get_date_range(period: str, target_date: date):
#     if period == 'day':
#         return target_date, target_date

#     if period == 'week':
#         start_of_week = target_date - timedelta(days=target_date.weekday())
#         end_of_week = start_of_week + timedelta(days=6)
#         return start_of_week, end_of_week

#     if period == 'month':
#         start_of_month = target_date.replace(day=1)
#         next_month = start_of_month.replace(day=28) + timedelta(days=4)
#         end_of_month = next_month - timedelta(days=next_month.day)
#         return start_of_month, end_of_month

#     if period == 'year':
#         start_of_year = target_date.replace(month=1, day=1)
#         end_of_year = target_date.replace(month=12, day=31)
#         return start_of_year, end_of_year

#     raise HTTPException(status_code=400, detail='Invalid period specified.')

# @router.get('/stores', response_model=List[schemas.Store])
# def list_stores():
#     """
#     Lấy danh sách tất cả các cửa hàng.
#     """
#     df = services.get_all_stores()
#     if df.empty:
#         return []

#     return df.to_dict(orient='records')

# @router.get('/errors', response_model=List[schemas.ErrorLog])
# def list_error_logs():
#     """
#     Lấy danh sách các log lỗi từ hệ thống.
#     """
#     df = services.get_error_logs()
#     if df.empty:
#         return []

#     return df.to_dict(orient='records')

# @router.get('/timeseries', response_model=schemas.TimeSeriesData)
# def get_timeseries_data(
#     period: str = Query('day', enum=['day', 'week', 'month', 'year']),
#     target_date_str: str = Query(..., alias='date'),
#     store_name: Optional[str] = Query(None)
# ):
#     """
#     Lấy dữ liệu chuỗi thời gian (cho biểu đồ line).
#     """
#     try:
#         target_date = date.fromisoformat(target_date_str)
#     except ValueError:
#         raise HTTPException(status_code=400, detail='Invalid date format. Use YYYY-MM-DD.')
        
#     start_date, end_date = get_date_range(period, target_date)
#     df = services.get_time_series_data(period, start_date, end_date, store_name)

#     if df.empty:
#         return schemas.TimeSeriesData(labels=[], data=[])

#     # Định dạng labels và data phù hợp
#     if period == 'day':
#         labels = [f"{h}:00" for h in df['hour']]
#         data = df['total_in'].tolist()
#     elif period == 'week':
#         day_map = {1: 'Chủ Nhật', 2: 'Thứ Hai', 3: 'Thứ Ba', 4: 'Thứ Tư', 5: 'Thứ Năm', 6: 'Thứ Sáu', 7: 'Thứ Bảy'}
#         labels = [day_map.get(d, 'Không rõ') for d in df['day_of_week']]
#         data = df['total_in'].tolist()
#     elif period == 'month':
#         labels = [f"Ngày {d}" for d in df['day']]
#         data = df['total_in'].tolist()
#     else: # year
#         labels = [f"Tháng {m}" for m in df['month']]
#         data = df['total_in'].tolist()

#     return TimeSeriesData(labels=labels, data=data)

# @router.get('/summary', response_model=schemas.SummaryMetrics)
# def get_summary_metrics(
#     period: str = Query('day', enum=['day', 'week', 'month', 'year']),
#     target_date_str: str = Query(..., alias='date'),
#     store_name: Optional[str] = Query(None)
# ):
#     """
#     Lấy các chỉ số tóm tắt (cho các thẻ thông tin).
#     """
#     try:
#         target_date = date.fromisoformat(target_date_str)
#     except ValueError:
#         raise HTTPException(status_code=400, detail='Invalid date format. Use YYYY-MM-DD.')

#     start_date, end_date = get_date_range(period, target_date)
#     df = services.get_summary_metrics_data(start_date, end_date, store_name)

#     if df.empty:
#         return schemas.SummaryMetrics(total_in=0, average_in=0, peak_time='--:--', occupancy=0, growth=0)

#     total_in = int(df['total_in'].sum())
#     occupancy = int(df['occupancy'].sum())

#     # Giờ cao điểm
#     peak_df = df.groupby('hour')['in_count'].sum().reset_index()
#     peak_hour = peak_df.loc[peak_df['in_count'].idxmax()]
#     peak_time = f"{int(peak_hour['hour'])}:00"

#     # Tăng trưởng so với kỳ trước
#     prev_start_date, _ = get_date_range(period, start_date - timedelta(days=1))
#     prev_end_date = start_date - timedelta(days=1)

#     prev_df = services.get_summary_metrics_data(prev_start_date, prev_end_date, store_name)
#     prev_total_in = int(prev_df['total_in'].sum())

#     growth = 0
#     if prev_total_in > 0:
#         growth = ((total_in - prev_total_in) / prev_total_in) * 100
#     elif total_in > 0:
#         growth = 100

#     return schemas.SummaryMetrics(
#         total_in=total_in,
#         average_in=total_in / len(df) if len(df) > 0 else 0,
#         peak_time=peak_time,
#         occupancy=occupancy,
#         growth=growth
#     )

# @router.get('/store-distribution', response_model=schemas.StoreDistribution)
# def get_store_distribution(
#     period: str = Query('day', enum=['day', 'week', 'month', 'year']),
#     target_date_str: str = Query(..., alias='date')
# ):
#     """
#     Lấy dữ liệu phân bổ lượt vào theo cửa hàng (cho biểu đồ tròn).
#     """
#     try:
#         target_date = date.fromisoformat(target_date_str)
#     except ValueError:
#         raise HTTPException(
#             status_code=400,
#             detail='Invalid date format. Use YYYY-MM-DD.'
#         )

#     start_date, end_date = get_date_range(period, target_date)
#     df = services.get_store_distribution_data(start_date, end_date)

#     if df.empty:
#         return schemas.StoreDistribution(labels=[], data=[])

#     return schemas.StoreDistribution(
#         labels=df['store_name'].tolist(),
#         data=df['total_in'].astype(int).tolist()
#     )

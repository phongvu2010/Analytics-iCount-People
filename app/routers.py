import logging

from datetime import date
from duckdb import DuckDBPyConnection
from fastapi import APIRouter, Depends, Query
from typing import List

from . import services
from .core.dependencies import get_db_connection
# from .schemas import DashboardResponse
from .services import DashboardService

# Khởi tạo một router mới
# prefix: tiền tố cho tất cả URL trong router này
# tags: nhóm các endpoint này lại trong tài liệu API
router = APIRouter(prefix='/api/v1', tags=['Dashboard'])
logger = logging.getLogger(__name__)

@router.get('/stores', response_model=List[str])
def get_stores(db: DuckDBPyConnection = Depends(get_db_connection)):
    """
    Endpoint lấy danh sách tất cả các vị trí (cửa hàng) có trong hệ thống.

    Dữ liệu này được dùng để khởi tạo bộ lọc (filter) trên giao diện người dùng.
    """
    return DashboardService.get_all_stores(db)

# @router.get('/dashboard', response_model=DashboardResponse)
# def get_dashboard_summary(
#     start_date: date = Query(..., description='Ngày bắt đầu, định dạng YYYY-MM-DD'),
#     end_date: date = Query(..., description='Ngày kết thúc, định dạng YYYY-MM-DD'),
#     period: str = Query('month', enum=['day', 'week', 'month', 'year'], description='Xem theo ngày, tuần, tháng, hoặc năm'),
#     store: str = Query('all', description='Lọc theo một vị trí cụ thể hoặc `all` cho tất cả'),
#     db: DuckDBPyConnection = Depends(get_db_connection)
# ):
#     """ Endpoint chính, trả về toàn bộ dữ liệu cần thiết để hiển thị dashboard. """
#     logger.info(
#         f"Nhận request dashboard: {start_date=} {end_date=} {period=} {store=}"
#     )
#     # Chuyển đổi date object thành string để truyền vào service
#     data = services.get_dashboard_data(db, str(start_date), str(end_date), period, store)
#     return data





# import asyncio

# from . import schemas

# router = APIRouter()

# def get_dashboard_service(
#     period: str = Query('day', description='Khoảng thời gian: `day`, `week`, `month`, `year`'),
#     start_date: date = Query(..., description='Ngày bắt đầu, định dạng: YYYY-MM-DD'),
#     end_date: date = Query(..., description='Ngày kết thúc, định dạng: YYYY-MM-DD'),
#     store: str = Query('all', description='Lọc theo cửa hàng hoặc `all` cho tất cả')
# ) -> DashboardService:
#     """
#     Dependency để khởi tạo và cung cấp DashboardService cho mỗi request.

#     Hàm này nhận các tham số query từ request, khởi tạo một instance của
#     DashboardService và inject vào các endpoint cần thiết.

#     Returns:
#         Một instance của DashboardService được cấu hình theo tham số request.
#     """
#     return DashboardService(period, start_date, end_date, store)

# @router.get('/dashboard', response_model=schemas.DashboardData)
# async def get_dashboard_data(
#     service: DashboardService = Depends(get_dashboard_service)
# ):
#     """
#     Cung cấp toàn bộ dữ liệu cho trang dashboard.

#     Endpoint này tổng hợp dữ liệu từ nhiều nguồn khác nhau bằng cách thực thi
#     các tác vụ I/O một cách song song để tối ưu hóa thời gian phản hồi.
#     """
#     # Các tác vụ I/O (query file) được thực thi đồng thời.
#     metrics_data, trend_data, store_data, table_data = await asyncio.gather(
#         service.get_metrics(),
#         service.get_trend_chart_data(),
#         service.get_store_comparison_chart_data(),
#         service.get_paginated_details(page=1, page_size=31)
#     )

#     # Các hàm static (đồng bộ) có thể được gọi tuần tự.
#     error_logs_data = DashboardService.get_error_logs()
#     latest_record_time_data = DashboardService.get_latest_record_time()

#     # Xây dựng đối tượng response hoàn chỉnh.
#     return schemas.DashboardData(
#         metrics = schemas.Metric(**metrics_data),
#         trend_chart = schemas.ChartData(series=trend_data),
#         store_comparison_chart = schemas.ChartData(series=store_data),
#         table_data = schemas.PaginatedTable(**table_data),
#         error_logs = error_logs_data,
#         latest_record_time = latest_record_time_data
#     )

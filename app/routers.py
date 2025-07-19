import asyncio
from datetime import date
from fastapi import APIRouter, Depends, Query
from typing import List

from . import schemas
from .services import DashboardService

router = APIRouter()

# Dependency này không cần thay đổi, FastAPI xử lý được cả sync và async dependencies
def get_dashboard_service(
    period: str = Query('day', description="Khoảng thời gian: 'day', 'week', 'month', 'year'"),
    start_date: date = Query(..., description="Ngày bắt đầu, định dạng: YYYY-MM-DD"),
    end_date: date = Query(..., description="Ngày kết thúc, định dạng: YYYY-MM-DD"),
    store: str = Query('all', description="Lọc theo cửa hàng hoặc 'all' cho tất cả")
) -> DashboardService:
    """Tạo một instance của DashboardService với các tham số từ query."""
    return DashboardService(period, start_date, end_date, store)

@router.get('/dashboard', response_model=schemas.DashboardData)
async def get_dashboard_data(  # <-- THAY ĐỔI: Chuyển sang `async def`
    service: DashboardService = Depends(get_dashboard_service),
):
    """
    Endpoint chính để cung cấp toàn bộ dữ liệu cho dashboard.
    Các tác vụ I/O được chạy song song để tối ưu hiệu năng.
    """
    # THAY ĐỔI: Sử dụng asyncio.gather để chạy các coroutine song song.
    # Điều này cho phép FastAPI gửi tất cả các query tới DuckDB gần như cùng lúc
    # và chờ tất cả hoàn thành, thay vì phải chờ từng query xong một.
    (
        metrics_data,
        trend_data,
        store_data,
        table_data,
    ) = await asyncio.gather(
        service.get_metrics(),
        service.get_trend_chart_data(),
        service.get_store_comparison_chart_data(),
        service.get_paginated_details(page=1, page_size=31), # Giữ nguyên phân trang mặc định
    )

    # Các hàm static (đồng bộ) có thể được gọi bình thường sau khi các tác vụ async hoàn tất
    error_logs_data = DashboardService.get_error_logs()
    latest_record_time_data = DashboardService.get_latest_record_time()

    # Dựng đối tượng response từ kết quả của các tác vụ
    return schemas.DashboardData(
        metrics=schemas.Metric(**metrics_data),
        trend_chart=schemas.ChartData(series=trend_data),
        store_comparison_chart=schemas.ChartData(series=store_data),
        table_data=schemas.PaginatedTable(**table_data),
        error_logs=error_logs_data,
        latest_record_time=latest_record_time_data
    )

@router.get('/stores', response_model=List[str])
def get_stores():
    """
    Endpoint để lấy danh sách tất cả các cửa hàng dùng cho dropdown filter.
    (Hàm này đơn giản, không cần chuyển sang async)
    """
    return DashboardService.get_all_stores()


# from datetime import date
# from fastapi import APIRouter, Depends
# from typing import List

# from . import schemas
# from .services import DashboardService

# router = APIRouter()

# def get_dashboard_service(
#     period: str, start_date: date, end_date: date, store: str = 'all'
# ) -> DashboardService:
#     return DashboardService(period, start_date, end_date, store)

# @router.get('/dashboard', response_model=schemas.DashboardData)
# def get_dashboard_data(
#     service: DashboardService = Depends(get_dashboard_service),
# ):
#     """
#     Endpoint chính để cung cấp toàn bộ dữ liệu cho dashboard,
#     bao gồm các chỉ số, dữ liệu biểu đồ, bảng và log lỗi.
#     """
#     metrics = service.get_metrics()
#     trend_data = service.get_trend_chart_data()
#     store_data = service.get_store_comparison_chart_data()
#     table_data = service.get_paginated_details(page=1, page_size=31)
#     error_logs = DashboardService.get_error_logs()
#     latest_record_time = DashboardService.get_latest_record_time()

#     return schemas.DashboardData(
#         metrics = schemas.Metric(**metrics),
#         trend_chart = schemas.ChartData(series=trend_data),
#         store_comparison_chart = schemas.ChartData(series=store_data),
#         table_data = schemas.PaginatedTable(**table_data),
#         error_logs = error_logs,
#         latest_record_time = latest_record_time
#     )

# @router.get('/stores', response_model=List[str])
# def get_stores():
#     """
#     Endpoint để lấy danh sách tất cả các cửa hàng dùng cho dropdown filter.
#     """
#     return DashboardService.get_all_stores()

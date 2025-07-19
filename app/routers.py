from datetime import date
from fastapi import APIRouter, Query, Depends
from typing import List

from . import schemas
from .services import DashboardService

router = APIRouter()

def get_dashboard_service(
    period: str,
    start_date: date,
    end_date: date,
    store: str = 'all'
) -> DashboardService:
    return DashboardService(period, start_date, end_date, store)

# @router.get('/dashboard', response_model=schemas.DashboardData)
# def get_dashboard_data(
#     service: DashboardService = Depends(get_dashboard_service), # Inject service ở đây
#     page: int = 1, page_size: int = Query(31, ge=1, le=100)
# ):
#     """
#     Endpoint chính để cung cấp toàn bộ dữ liệu cho dashboard,
#     bao gồm các chỉ số, dữ liệu biểu đồ, bảng và log lỗi.
#     """
#     metrics = service.get_metrics()
#     trend_data = service.get_trend_chart_data()
#     store_data = service.get_store_comparison_chart_data()
#     table_data = service.get_paginated_details(page, page_size)
#     error_logs = DashboardService.get_error_logs()

#     return schemas.DashboardData(
#         metrics=schemas.Metric(**metrics),
#         trend_chart=schemas.ChartData(series=trend_data),
#         store_comparison_chart=schemas.ChartData(series=store_data),
#         table_data=schemas.PaginatedTable(**table_data),
#         error_logs=error_logs
#     )

@router.get('/dashboard', response_model=schemas.DashboardData)
def get_dashboard_data(
    service: DashboardService = Depends(get_dashboard_service)
):
    """
    Endpoint chính để cung cấp toàn bộ dữ liệu cho dashboard.
    """
    metrics = service.get_metrics()
    trend_data = service.get_trend_chart_data()
    store_data = service.get_store_comparison_chart_data()
    # GỌI HÀM VỚI CÁC GIÁ TRỊ CỐ ĐỊNH
    table_data = service.get_paginated_details(page=1, page_size=31)
    error_logs = DashboardService.get_error_logs()
    # THÊM MỚI: Gọi hàm lấy thời gian gần nhất
    latest_record_time = DashboardService.get_latest_record_time()

    return schemas.DashboardData(
        metrics=schemas.Metric(**metrics),
        trend_chart=schemas.ChartData(series=trend_data),
        store_comparison_chart=schemas.ChartData(series=store_data),
        table_data=schemas.PaginatedTable(**table_data),
        error_logs=error_logs,
        latest_record_time=latest_record_time
    )

@router.get('/stores', response_model=List[str])
def get_stores():
    """
    Endpoint để lấy danh sách tất cả các cửa hàng
    dùng cho dropdown filter.
    """
    return DashboardService.get_all_stores()

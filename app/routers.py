import logging

from datetime import date
from duckdb import DuckDBPyConnection
from fastapi import APIRouter, Depends, Query

from . import services
from .dependencies import get_db_connection
from .schemas import DashboardResponse

# Khởi tạo một router mới
# prefix: tiền tố cho tất cả URL trong router này
# tags: nhóm các endpoint này lại trong tài liệu API
router = APIRouter(prefix='/api/v1', tags=['Dashboard'])
logger = logging.getLogger(__name__)

@router.get('/stores', response_model=list[str])
def get_all_stores(db: DuckDBPyConnection = Depends(get_db_connection)):
    """ Endpoint để lấy danh sách tất cả các vị trí (cửa hàng) có trong hệ thống. """
    return services.get_store_names(db)

@router.get('/dashboard', response_model=DashboardResponse)
def get_dashboard_summary(
    start_date: date = Query(..., description='Ngày bắt đầu, định dạng YYYY-MM-DD'),
    end_date: date = Query(..., description='Ngày kết thúc, định dạng YYYY-MM-DD'),
    period: str = Query('month', enum=['day', 'week', 'month', 'year'], description='Xem theo ngày, tuần, tháng, hoặc năm'),
    store: str = Query('all', description='Lọc theo một vị trí cụ thể hoặc `all` cho tất cả'),
    db: DuckDBPyConnection = Depends(get_db_connection)
):
    """ Endpoint chính, trả về toàn bộ dữ liệu cần thiết để hiển thị dashboard. """
    logger.info(f"Nhận request dashboard: {start_date=} {end_date=} {period=} {store=}")

    # Chuyển đổi date object thành string để truyền vào service
    data = services.get_dashboard_data(db, str(start_date), str(end_date), period, store)
    return data

from datetime import datetime
from pydantic import BaseModel
from typing import List, Dict, Optional, Any

# Dùng cho các thẻ metric (giữ nguyên)
class Metric(BaseModel):
    total_in: int
    average_in: float
    peak_time: Optional[str]
    current_occupancy: int
    busiest_store: Optional[str]
    growth: float

# Dùng cho biểu đồ (giữ nguyên)
class ChartDataPoint(BaseModel):
    x: Any
    y: int

class ChartData(BaseModel):
    series: List[ChartDataPoint]

# --- THAY ĐỔI Ở ĐÂY ---
# Bỏ model TableRow cũ và thay bằng model cho dữ liệu tổng hợp
class SummaryTableRow(BaseModel):
    period: str
    total_in: int
    pct_change: float

# # Cập nhật PaginatedTable để sử dụng model mới
# class PaginatedTable(BaseModel):
#     total_records: int
#     page: int
#     page_size: int
#     data: List[SummaryTableRow] # <-- Thay đổi từ List[TableRow] thành List[SummaryTableRow]
# # --- KẾT THÚC THAY ĐỔI ---
class PaginatedTable(BaseModel):
    total_records: int
    page: int
    page_size: int
    data: List[SummaryTableRow]
    summary: Dict[str, Any] # <-- THÊM DÒNG NÀY

# Dùng cho log lỗi (giữ nguyên)
class ErrorLog(BaseModel):
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str

# Model tổng hợp cho response của API dashboard (giữ nguyên)
class DashboardData(BaseModel):
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: PaginatedTable
    error_logs: List[ErrorLog]

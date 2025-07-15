from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional, Any

# Dùng cho các thẻ metric
class Metric(BaseModel):
    total_in: int
    average_in: float
    peak_time: Optional[str]
    current_occupancy: int
    busiest_store: Optional[str]
    growth: float

# Dùng cho biểu đồ (time-series và donut)
class ChartDataPoint(BaseModel):
    x: Any
    y: int

class ChartData(BaseModel):
    series: List[ChartDataPoint]

# Dùng cho một dòng trong bảng dữ liệu
class TableRow(BaseModel):
    record_time: datetime
    store_name: str
    in_count: int
    out_count: int

# Dùng cho bảng dữ liệu có phân trang
class PaginatedTable(BaseModel):
    total_records: int
    page: int
    page_size: int
    data: List[TableRow]

# Dùng cho log lỗi
class ErrorLog(BaseModel):
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str

# Model tổng hợp cho response của API dashboard
class DashboardData(BaseModel):
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: PaginatedTable
    error_logs: List[ErrorLog]

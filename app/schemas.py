from pydantic import BaseModel, Field
from typing import List, Optional

# --- Schemas cho các chỉ số chính ---
class DashboardMetrics(BaseModel):
    total_in: int = Field(..., description='Tổng lượt khách vào trong kỳ.')
    average_in: float = Field(..., description='Trung bình lượt khách vào mỗi ngày/tuần/tháng.')
    peak_time: Optional[str] = Field(None, description='Khung giờ cao điểm trong ngày (HH:00).')
    busiest_store: Optional[str] = Field(None, description='Cửa hàng/vị trí đông nhất.')
    growth: float = Field(..., description='Tỷ lệ tăng trưởng so với kỳ trước.')

# --- Schemas cho dữ liệu biểu đồ ---
class ChartDataPoint(BaseModel):
    x: str | int # Trục hoành (thời gian, tên cửa hàng,...)
    y: int | float # Trục tung (giá trị)

class Chart(BaseModel):
    series: List[ChartDataPoint]

# --- Schemas cho bảng dữ liệu chi tiết ---
class TableRow(BaseModel):
    period: str = Field(..., description='Kỳ báo cáo (ngày, tuần, tháng, năm).')
    total_in: int = Field(..., description='Tổng lượt vào trong kỳ đó.')
    pct_change: float = Field(..., description='Phần trăm thay đổi so với kỳ trước.')

class TableData(BaseModel):
    data: List[TableRow]
    summary: dict

# --- Schema cho log lỗi ---
class ErrorLogEntry(BaseModel):
    log_time: str
    store_name: str
    error_code: Optional[int]
    error_message: Optional[str]

# --- Schema tổng hợp cho toàn bộ Dashboard ---
class DashboardResponse(BaseModel):
    metrics: DashboardMetrics
    trend_chart: Chart
    store_comparison_chart: Chart
    table_data: TableData
    error_logs: List[ErrorLogEntry]
    latest_record_time: Optional[str]




# from datetime import datetime
# from typing import Dict, Any

# class Metric(BaseModel):
#     """ Định nghĩa cấu trúc cho các thẻ chỉ số (KPIs) chính trên dashboard. """
#     total_in: int
#     average_in: float
#     peak_time: Optional[str]
#     current_occupancy: int
#     busiest_store: Optional[str]
#     growth: float

# class ChartDataPoint(BaseModel):
#     """ Định nghĩa một điểm dữ liệu duy nhất trên biểu đồ (ví dụ: một cột, một điểm). """
#     x: Any  # Trục hoành, có thể là ngày, giờ, hoặc tên cửa hàng
#     y: int  # Trục tung, thường là giá trị số (ví dụ: lượt khách)

# class ChartData(BaseModel):
#     """ Định nghĩa dữ liệu cho một biểu đồ hoàn chỉnh. """
#     series: List[ChartDataPoint]

# class SummaryTableRow(BaseModel):
#     """ Định nghĩa cấu trúc cho một hàng trong bảng dữ liệu chi tiết. """
#     period: str
#     total_in: int
#     pct_change: float

# class PaginatedTable(BaseModel):
#     """ Định nghĩa cấu trúc cho toàn bộ bảng dữ liệu có phân trang. """
#     total_records: int
#     page: int
#     page_size: int
#     data: List[SummaryTableRow]
#     summary: Dict[str, Any]

# class ErrorLog(BaseModel):
#     """ Định nghĩa cấu trúc cho một bản ghi log lỗi. """
#     id: int
#     store_name: str
#     log_time: datetime
#     error_code: int
#     error_message: str

# class DashboardData(BaseModel):
#     """
#     Model tổng hợp, định nghĩa cấu trúc response cuối cùng cho API dashboard.

#     Đây là đối tượng dữ liệu chính mà frontend sẽ nhận và sử dụng để hiển thị
#     toàn bộ thông tin trên trang.
#     """
#     metrics: Metric
#     trend_chart: ChartData
#     store_comparison_chart: ChartData
#     table_data: PaginatedTable
#     error_logs: List[ErrorLog]
#     latest_record_time: Optional[datetime] = None

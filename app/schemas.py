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
    x: str | int    # Trục hoành (thời gian, tên cửa hàng,...)
    y: int | float  # Trục tung (giá trị)

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

"""
Định nghĩa các Pydantic model (schema) cho việc xác thực dữ liệu API.

Các model này đóng vai trò là "hợp đồng dữ liệu" (data contract), đảm bảo
rằng dữ liệu được gửi đi và nhận về qua API luôn tuân thủ một cấu trúc
nhất quán và đúng kiểu dữ liệu. FastAPI sử dụng các model này để tự động
xác thực request, tuần tự hóa response và tạo tài liệu API (Swagger/OpenAPI).
"""
from datetime import datetime
from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class Metric(BaseModel):
    """
    Định nghĩa cấu trúc cho các thẻ chỉ số (KPIs) chính trên dashboard.
    """
    total_in: int
    average_in: float
    peak_time: Optional[str] = None
    current_occupancy: int
    busiest_store: Optional[str] = None
    growth: float


class ChartDataPoint(BaseModel):
    """
    Định nghĩa một điểm dữ liệu duy nhất trên biểu đồ.
    """
    x: Any  # Trục hoành: có thể là ngày, giờ, hoặc tên cửa hàng (str)
    y: int  # Trục tung: giá trị số (ví dụ: lượt khách)


class ChartData(BaseModel):
    """
    Định nghĩa dữ liệu cho một biểu đồ hoàn chỉnh.
    """
    series: List[ChartDataPoint]


class SummaryTableRow(BaseModel):
    """
    Định nghĩa cấu trúc cho một hàng trong bảng dữ liệu chi tiết.
    """
    period: str
    total_in: int
    pct_change: float
    proportion_pct: float
    proportion_change: float


class TableData(BaseModel):
    """
    Định nghĩa cấu trúc cho toàn bộ dữ liệu của bảng.
    """
    data: List[SummaryTableRow]
    summary: Dict[str, Any]


class ErrorLog(BaseModel):
    """
    Định nghĩa cấu trúc cho một bản ghi log lỗi.
    """
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str


class DashboardData(BaseModel):
    """
    Model tổng hợp, định nghĩa cấu trúc response cuối cùng cho API dashboard.

    Đây là đối tượng dữ liệu chính mà frontend sẽ nhận và sử dụng để hiển thị
    toàn bộ thông tin trên trang.
    """
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: TableData
    error_logs: List[ErrorLog]
    latest_record_time: Optional[datetime] = None

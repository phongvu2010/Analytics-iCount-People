from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# # ===========================================================
# # Schema cho thông tin cửa hàng
# # ===========================================================
class Store(BaseModel):
    tid: int
    name: str
    class Config:
        from_attributes = True # Cho phép Pydantic đọc dữ liệu từ các thuộc tính object

# # ===========================================================
# # Schema cho một bản ghi lỗi
# # ===========================================================
class ErrorLog(BaseModel):
    ID: int
    storeid: int
    LogTime: datetime
    Errorcode: Optional[int] = None
    ErrorMessage: Optional[str] = None
    class Config:
        from_attributes = True

# # ===========================================================
# # Schema cho một điểm dữ liệu trong bảng tổng hợp
# # ===========================================================
class SummaryTableRow(BaseModel):
    label: str
    value: int

# # ===========================================================
# # Schema cho dữ liệu biểu đồ đường
# # ===========================================================
class LineChartData(BaseModel):
    labels: List[str]
    values: List[int]

# # ===========================================================
# # Schema cho dữ liệu biểu đồ tròn
# # ===========================================================
class DonutChartData(BaseModel):
    labels: List[str]
    values: List[int]

# # ===========================================================
# # Schema tổng hợp cho toàn bộ dữ liệu trả về cho dashboard
# # ===========================================================
class DashboardData(BaseModel):
    line_chart: LineChartData
    donut_chart: DonutChartData
    summary_table: List[SummaryTableRow]

# # ===========================================================
# # Schema cho dữ liệu chi tiết để tải xuống
# # ===========================================================
class DetailedDataRow(BaseModel):
    recordtime: datetime
    in_num: int
    store_name: str








# # ============================================
# # Schemas cho dữ liệu trả về của API
# # ============================================

# class TimeSeriesDataPoint(BaseModel):
#     """
#     Đại diện cho một điểm dữ liệu trên biểu đồ thời gian.
#     Ví dụ: {'period': '2024-07-10', 'in_count': 500}
#     """
#     period: str  # Có thể là ngày, tháng, tuần, v.v.
#     in_count: int

# class StoreComparisonDataPoint(BaseModel):
#     """
#     Đại diện cho một phần trong biểu đồ Donut so sánh các cửa hàng.
#     Ví dụ: {'store_name': 'Cửa chính', 'total_in': 12000}
#     """
#     store_name: str
#     total_in: int

# class AggregatedTableRow(BaseModel):
#     """
#     Đại diện cho một hàng trong bảng dữ liệu tổng hợp.
#     """
#     period: str
#     total_in: int
#     percentage_change: Optional[float] = None # Chênh lệch so với dòng trước

# class DashboardDataResponse(BaseModel):
#     """
#     Schema tổng hợp cho toàn bộ dữ liệu cần thiết trên dashboard.
#     Đây là đối tượng chính mà API sẽ trả về.
#     """
#     time_series_data: List[TimeSeriesDataPoint]
#     store_comparison_data: List[StoreComparisonDataPoint]
#     aggregated_table_data: List[AggregatedTableRow]
#     error_logs: List[ErrLogBase]
#     stores: List[StoreBase]

# class DetailDataRow(BaseModel):
#     """Schema cho dữ liệu chi tiết khi xuất file."""
#     recordtime: datetime.datetime
#     in_num: int
#     out_num: int
#     position: Optional[str] = None
#     store_name: str

# # --- API Response Schemas ---
# class StatDataItem(BaseModel):
#     label: str
#     value: float

# class StatsResponse(BaseModel):
#     period: str
#     labels: List[str]
#     data: List[float]
#     total_in: float
#     average_in: float
#     max_in: float
#     growth: float
#     time_range: str

# class DonutChartData(BaseModel):
#     labels: List[str]
#     data: List[float]

# class TableDataItem(BaseModel):
#     label: str
#     value: float
#     difference: float = Field(..., description="Chênh lệch so với dòng trước (%)")

# class FullStatsResponse(BaseModel):
#     line_chart: StatsResponse
#     donut_chart: DonutChartData
#     table_data: List[TableDataItem]

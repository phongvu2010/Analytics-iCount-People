from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional

# Schema cho dữ liệu thống kê trả về
class StatData(BaseModel):
    label: str
    value: int

# Schema cho dữ liệu Donut Chart
class DonutData(BaseModel):
    label: str
    value: int

# Schema cho các chỉ số chính
class Metrics(BaseModel):
    total_in: int
    average_in: float
    max_in: int
    growth: float

# Schema cho response chính của API stats
class StatsResponse(BaseModel):
    period: str
    time_range_display: str
    metrics: Metrics
    chart_data: List[StatData]
    donut_data: List[DonutData]
    table_data: List[StatData] # Dữ liệu chi tiết cho table

# Schema cho log lỗi
class ErrorLog(BaseModel):
    ID: int
    storeid: int
    DeviceCode: Optional[int]
    LogTime: datetime
    Errorcode: Optional[int]
    ErrorMessage: Optional[str]

    class Config:
        from_attributes = True

# Schema cho cửa hàng
class Store(BaseModel):
    tid: int
    name: str

    class Config:
        from_attributes = True

# from pydantic import BaseModel
# from typing import List, Optional, Any
# from datetime import datetime

# # =================================================================
# # Schemas cho các đối tượng cơ sở (tương ứng với bảng DB)
# # =================================================================
# class StoreBase(BaseModel):
#     """ Schema cơ sở cho cửa hàng. """
#     tid: int
#     name: str

# class Store(StoreBase):
#     """ Schema để hiển thị thông tin cửa hàng. """
#     class Config:
#         from_attributes = True # Trước đây là orm_mode

# class ErrorLogBase(BaseModel):
#     """ Schema cơ sở cho log lỗi. """
#     ID: int
#     storeid: int
#     LogTime: datetime
#     ErrorMessage: Optional[str] = None

# class ErrorLog(ErrorLogBase):
#     """ Schema để hiển thị log lỗi, có thêm tên cửa hàng. """
#     store_name: Optional[str] = 'Không rõ'
#     class Config:
#         from_attributes = True

# # =================================================================
# # Schemas cho các Response của API
# # =================================================================
# class DataPoint(BaseModel):
#     """
#     Schema cho một điểm dữ liệu trên biểu đồ chính.
#     Ví dụ: { "label": "Tháng 1", "value": 15000 }
#     """
#     label: str
#     value: int

# class PieChartDataPoint(BaseModel):
#     """
#     Schema cho một điểm dữ liệu trên biểu đồ tròn (tỷ trọng).
#     Ví dụ: { "label": "Cửa chính A1", "value": 50000 }
#     """
#     label: str
#     value: int

# class GrowthData(BaseModel):
#     """ Schema cho dữ liệu tăng trưởng. """
#     percentage: float
#     status: str # 'increase', 'decrease', 'stable'

# class TrafficMetrics(BaseModel):
#     """ Schema cho các chỉ số chính hiển thị trên dashboard. """
#     total_in: int
#     average_in: float
#     max_in: int
#     growth: GrowthData

# class TrafficDataResponse(BaseModel):
#     """
#     Schema cho response chính của API traffic-data.
#     Đây là cấu trúc dữ liệu hoàn chỉnh mà frontend sẽ nhận được.
#     """
#     metrics: TrafficMetrics
#     main_chart_data: List[DataPoint]
#     pie_chart_data: List[PieChartDataPoint]
#     table_data: List[DataPoint] # Dữ liệu cho bảng chi tiết
#     time_range_display: str

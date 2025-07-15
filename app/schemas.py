from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional


# Dữ liệu cho một điểm trên biểu đồ
class TrafficDataPoint(BaseModel):
    label: str  # Ví dụ: "14:00", "Thứ Hai", "Ngày 15", "Tháng 7"
    value: int

# Các chỉ số tổng quan
class DashboardMetrics(BaseModel):
    total_in: int
    average_in: float
    peak_time: str
    occupancy: int
    busiest_store: str
    growth_percentage: float

# Dữ liệu cho biểu đồ tròn so sánh các cửa
class StoreComparison(BaseModel):
    labels: List[str]
    data: List[int]

# Dữ liệu đầy đủ cho Dashboard
class DashboardData(BaseModel):
    metrics: DashboardMetrics
    line_chart_data: List[TrafficDataPoint]
    store_comparison_data: StoreComparison
    table_data: List[TrafficDataPoint] # Có thể tái sử dụng TrafficDataPoint cho bảng

# Dữ liệu cho một cảnh báo lỗi
class ErrorLog(BaseModel):
    store_name: str
    log_time: datetime
    error_message: str

# Dữ liệu cho một cửa hàng
class Store(BaseModel):
    id: int
    name: str










# class Store(BaseModel):
#     store_name: str

# class ErrorLog(BaseModel):
#     id: int
#     store_name: str
#     log_time: datetime
#     error_code: int
#     error_message: str

#     class Config:
#         from_attributes = True

# class TimeSeriesData(BaseModel):
#     labels: List[str]
#     data: List[int]

# class SummaryMetrics(BaseModel):
#     total_in: int
#     # total_out: int
#     average_in: float
#     # average_out: float
#     peak_time: str
#     occupancy: int
#     growth: float

# class StoreDistribution(BaseModel):
#     labels: List[str]
#     data: List[int]

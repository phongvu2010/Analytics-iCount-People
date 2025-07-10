from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import List, Optional, Any

# --- Schemas cho Dữ liệu Thống kê Trả về ---
class StatDataPoint(BaseModel):
    """
    Đại diện cho một điểm dữ liệu trên biểu đồ đường (line chart).
    """
    label: str
    value: int

# class StatsResponse(BaseModel):
#     period: str
#     data: List[StatData]

class TableDataRow(BaseModel):
    """
    Đại diện cho một hàng trong bảng dữ liệu tổng hợp.
    """
    label: str
    value: int
    percentage_change: Optional[float] = Field(None, description="Tỷ lệ % thay đổi so với dòng trước đó")

class DonutChartDataPoint(BaseModel):
    """
    Đại diện cho một lát cắt trên biểu đồ donut, so sánh giữa các cửa.
    """
    store_name: str
    value: int

class StatsResponse(BaseModel):
    """
    Schema cho phản hồi của API thống kê chính.
    Bao gồm dữ liệu cho biểu đồ đường, bảng và biểu đồ donut.
    """
    line_chart_data: List[StatDataPoint]
    table_data: List[TableDataRow]
    donut_chart_data: List[DonutChartDataPoint]

# --- Schema cho Log Lỗi ---
class ErrorLog(BaseModel):
    """
    Schema cho một bản ghi log lỗi.
    """
    ID: int
    storeid: int
    store_name: Optional[str] = None # Thêm tên cửa hàng để hiển thị thân thiện hơn
    DeviceCode: Optional[int] = None
    LogTime: datetime
    Errorcode: Optional[int] = None
    ErrorMessage: Optional[str] = None

    class Config:
        from_attributes = True # Giúp Pydantic tương thích với các đối tượng ORM/DB

# --- Schema cho Cửa hàng (Store) ---
class Store(BaseModel):
    """
    Schema cho thông tin một cửa hàng.
    """
    tid: int
    name: str

    class Config:
        from_attributes = True

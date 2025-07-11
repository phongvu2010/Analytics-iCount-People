# # === FILENAME: schemas.py ===
# # Module định nghĩa các schema Pydantic để validate và serialize dữ liệu.


# # Pydantic schemas (validate dữ liệu)


# # =======================================
# # Schemas cho Token (Xác thực)
# # =======================================
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# # =======================================
# # Schemas cho Store
# # =======================================
# # Schema cơ bản cho Store
# class StoreBase(BaseModel):
#     name: str

# # Schema dùng để trả về thông tin Store từ API
# class Store(StoreBase):
#     tid: int

#     class Config:
#         orm_mode = True # Cho phép Pydantic đọc dữ liệu từ ORM model

# # =======================================
# # Schemas cho NumCrowd
# # =======================================
# class NumCrowdBase(BaseModel):
#     recordtime: datetime
#     in_num: int
#     out_num: int
#     position: Optional[str] = None
#     storeid: int

# class NumCrowd(NumCrowdBase):
#     class Config:
#         orm_mode = True

# # =======================================
# # Schemas cho ErrLog
# # =======================================
# class ErrLogBase(BaseModel):
#     storeid: int
#     LogTime: datetime
#     DeviceCode: Optional[int] = None
#     Errorcode: Optional[int] = None
#     ErrorMessage: Optional[str] = None

# class ErrLog(ErrLogBase):
#     ID: int

#     class Config:
#         orm_mode = True









# # # --- Pydantic Models (dùng cho API request/response) ---
# # class StoreSchema(BaseModel):
# #     tid: int
# #     name: str

# #     class Config:
# #         from_attributes = True

# # class ErrLogSchema(BaseModel):
# #     store_name: Optional[str] = 'N/A'
# #     log_time: datetime
# #     # error_code: str
# #     error_message: str

# #     class Config:
# #         from_attributes = True










# Pydantic schemas (validate dữ liệu)

# =======================================
# Schemas cho Token (Xác thực)
# =======================================
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# Schema cơ bản cho Store
class StoreBase(BaseModel):
    name: str

# Schema dùng để trả về thông tin Store từ API
class Store(StoreBase):
    tid: int

    class Config:
        orm_mode = True # Cho phép Pydantic đọc dữ liệu từ ORM model

# =======================================
# Schemas cho NumCrowd
# =======================================
class NumCrowdBase(BaseModel):
    recordtime: datetime
    in_num: int
    out_num: int
    position: Optional[str] = None
    storeid: int

class NumCrowd(NumCrowdBase):
    class Config:
        orm_mode = True

class ErrLogBase(BaseModel):
    storeid: int
    LogTime: datetime
    DeviceCode: Optional[int] = None
    Errorcode: Optional[int] = None
    ErrorMessage: Optional[str] = None

class ErrLog(ErrLogBase):
    ID: int

    class Config:
        orm_mode = True












# # ======== Token Schemas ========
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# # ======== Crowd Data Schemas ========
# class NumCrowd(BaseModel):
#     recordtime: datetime
#     in_num: int
#     out_num: int
#     class Config:
#         from_attributes = True

# # ======== Aggregated Data Schema ========
# class AggregatedCrowdData(BaseModel):
#     period: str # Sẽ là ngày, tuần, hoặc tháng
#     in_num: int
#     out_num: int




from datetime import datetime
from pydantic import BaseModel
# from typing import List, Optional

# =======================================
# Schemas cho Store
# =======================================
class Store(BaseModel):
    tid: int
    name: str
    class Config:
        orm_mode = True

# =======================================
# Schemas cho ErrLog
# =======================================
class ErrLog(BaseModel):
    ID: int
    storeid: int
    LogTime: datetime
    ErrorMessage: str
    class Config:
        orm_mode = True








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

from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

# Schema cho dữ liệu thống kê trả về
class StatData(BaseModel):
    label: str
    value: int

class StatsResponse(BaseModel):
    period: str
    data: List[StatData]

# Schema cho log lỗi
class ErrorLog(BaseModel):
    ID: int
    storeid: int
    DeviceCode: Optional[int]
    LogTime: datetime
    Errorcode: Optional[int]
    ErrorMessage: Optional[str]

    class Config:
        orm_mode = True # Giúp Pydantic tương thích với các đối tượng ORM/DB

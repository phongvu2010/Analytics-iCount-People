from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String #, DateTime, Float
from typing import List, Optional

from .core.database import Base

# --- SQLAlchemy Models (ánh xạ tới bảng trong DB) ---
class Store(Base):
    __tablename__ = 'store'
    __table_args__ = {'schema': 'dbo'}  # Ánh xạ tới schema dbo.store

    tid = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    # crowd_data = relationship('NumCrowd', back_populates='store')
    # error_logs = relationship('ErrLog', back_populates='store')

# class NumCrowd(Base):
#     __tablename__ = "num_crowd"
#     __table_args__ = {'schema': 'dbo'}

#     recordtime = Column(DateTime, primary_key=True, index=True)
#     in_num = Column(Integer)
#     out_num = Column(Integer)
#     storeid = Column(Integer, primary_key=True)

# class ErrLog(Base):
#     __tablename__ = "ErrLog"
#     __table_args__ = {'schema': 'dbo'}

#     ID = Column(Integer, primary_key=True, index=True)
#     storeid = Column(Integer)
#     LogTime = Column(DateTime, index=True)
#     Errorcode = Column(String)
#     ErrorMessage = Column(String)


# --- Pydantic Models (dùng cho API request/response) ---
class StoreSchema(BaseModel):
    tid: int
    name: str

    class Config:
        from_attributes = True

class ErrLogSchema(BaseModel):
    store_name: Optional[str] = 'N/A'
    log_time: datetime
    # error_code: str
    error_message: str

    class Config:
        from_attributes = True

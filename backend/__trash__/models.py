from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, Float
from typing import List, Optional

from .core.database import Base

# --- SQLAlchemy Models (ánh xạ tới bảng trong DB) ---
class Store(Base):
    __tablename__ = 'store'
    # Ánh xạ tới schema dbo.store
    __table_args__ = {'schema': 'dbo'}

    tid = Column(Integer, primary_key=True, index=True)
    name = Column(String)

class NumCrowd(Base):
    __tablename__ = 'num_crowd'
    # Ánh xạ tới schema dbo.num_crowd
    __table_args__ = {'schema': 'dbo'}

    recordtime = Column(DateTime, primary_key=True, index=True)
    in_num = Column(Integer)
    out_num = Column(Integer)
    storeid = Column(Integer, primary_key=True)

class ErrLog(Base):
    __tablename__ = 'ErrLog'
    # Ánh xạ tới schema dbo.ErrLog
    __table_args__ = {'schema': 'dbo'}

    ID = Column(Integer, primary_key=True, index=True)
    storeid = Column(Integer)
    LogTime = Column(DateTime, index=True)
    Errorcode = Column(String)
    ErrorMessage = Column(String)

# --- Pydantic Models (dùng cho API request/response) ---
class StoreSchema(BaseModel):
    tid: int
    name: str

    class Config:
        orm_mode = True

class ErrLogSchema(BaseModel):
    store_name: Optional[str] = "N/A"
    log_time: datetime
    error_message: str

    class Config:
        orm_mode = True

















# from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
# from sqlalchemy.orm import relationship


# class Store(Base):
#     __tablename__ = 'store'
#     # Ánh xạ tới cột `tid` nhưng dùng `id` trong code cho tiện
#     tid = Column(Integer, primary_key=True, index=True)
#     name = Column(String)

#     crowd_data = relationship('NumCrowd', back_populates='store')
#     error_logs = relationship('ErrLog', back_populates='store')

# class NumCrowd(Base):
#     __tablename__ = 'num_crowd'
#     # id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id để dễ quản lý
#     recordtime = Column(DateTime, index=True)
#     in_num = Column(Integer)
#     out_num = Column(Integer)
#     storeid = Column(Integer, ForeignKey('store.tid'))

#     store = relationship('Store', back_populates='crowd_data')

# class ErrLog(Base):
#     __tablename__ = 'ErrLog'
#     # id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id
#     storeid = Column(Integer, ForeignKey('store.tid'))
#     LogTime = Column(DateTime)
#     Errorcode = Column(String)
#     ErrorMessage = Column(String)

#     store = relationship('Store', back_populates='error_logs')

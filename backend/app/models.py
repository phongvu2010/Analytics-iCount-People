# from sqlalchemy import Column, Integer, String, DateTime, Float
# from pydantic import BaseModel
# from datetime import datetime
# from typing import List, Optional

# from .core.database import Base

# # --- SQLAlchemy Models (ánh xạ tới bảng trong DB) ---
# class Store(Base):
#     __tablename__ = "store"
#     # Ánh xạ tới schema dbo.store
#     __table_args__ = {'schema': 'dbo'}
    
#     tid = Column(Integer, primary_key=True, index=True)
#     name = Column(String)

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


# # --- Pydantic Models (dùng cho API request/response) ---
# class StoreSchema(BaseModel):
#     tid: int
#     name: str

#     class Config:
#         orm_mode = True

# class ErrLogSchema(BaseModel):
#     store_name: Optional[str] = "N/A"
#     log_time: datetime
#     error_message: str

#     class Config:
#         orm_mode = True

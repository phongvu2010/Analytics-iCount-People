# from datetime import datetime
from sqlalchemy import Column, Integer, String #, DateTime, Float, ForeignKey
# from sqlalchemy.orm import relationship
# from typing import List, Optional

from .core.database import Base

# --- SQLAlchemy Models (ánh xạ tới bảng trong DB) ---
class Store(Base):
    __tablename__ = 'store'
    # Ánh xạ tới schema dbo.store
    __table_args__ = {'schema': 'dbo'}

    tid = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    # crowd_data = relationship('NumCrowd', back_populates='store')
    # error_logs = relationship('ErrLog', back_populates='store')

# class NumCrowd(Base):
#     __tablename__ = 'num_crowd'
#     # Ánh xạ tới schema dbo.num_crowd
#     __table_args__ = {'schema': 'dbo'}

#     recordtime = Column(DateTime, primary_key=True, index=True)
#     in_num = Column(Integer)
#     out_num = Column(Integer)
#     storeid = Column(Integer, primary_key=True)

#     # id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id để dễ quản lý
#     recordtime = Column(DateTime, index=True)
#     storeid = Column(Integer, ForeignKey('store.tid'))

#     store = relationship('Store', back_populates='crowd_data')

# class ErrLog(Base):
#     __tablename__ = 'ErrLog'
#     # Ánh xạ tới schema dbo.ErrLog
#     __table_args__ = {'schema': 'dbo'}

#     ID = Column(Integer, primary_key=True, index=True)
#     storeid = Column(Integer)
#     LogTime = Column(DateTime, index=True)
#     Errorcode = Column(String)
#     ErrorMessage = Column(String)

#     # id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id
#     storeid = Column(Integer, ForeignKey('store.tid'))
#     LogTime = Column(DateTime)

#     store = relationship('Store', back_populates='error_logs')

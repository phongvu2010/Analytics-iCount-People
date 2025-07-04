from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from ..core.database import Base

class Store(Base):
    __tablename__ = "store"
    # Ánh xạ tới cột `tid` nhưng dùng `id` trong code cho tiện
    id = Column("tid", Integer, primary_key=True, index=True)
    name = Column(String)
    
    crowd_data = relationship("NumCrowd", back_populates="store")
    error_logs = relationship("ErrLog", back_populates="store")

class NumCrowd(Base):
    __tablename__ = "num_crowd"
    id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id để dễ quản lý
    recordtime = Column(DateTime, index=True)
    in_num = Column(Integer)
    out_num = Column(Integer)
    storeid = Column(Integer, ForeignKey("store.tid"))
    
    store = relationship("Store", back_populates="crowd_data")

class ErrLog(Base):
    __tablename__ = "ErrLog"
    id = Column(Integer, primary_key=True, index=True) # Giả sử có cột id
    storeid = Column(Integer, ForeignKey("store.tid"))
    LogTime = Column(DateTime)
    Errorcode = Column(String)
    ErrorMessage = Column(String)

    store = relationship("Store", back_gpopulates="error_logs")

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)

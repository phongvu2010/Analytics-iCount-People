# # === FILENAME: models.py ===
# # Module định nghĩa các model SQLAlchemy tương ứng với cấu trúc CSDL.

# from sqlalchemy import Column, Integer, String, DateTime, SmallInteger, BigInteger, ForeignKey
# from app.core.database import Base

# class Store(Base):
#     __tablename__ = "store"
#     tid = Column(Integer, primary_key=True, index=True)
#     name = Column(String(80), nullable=False)

# class ErrLog(Base):
#     __tablename__ = "ErrLog"
#     ID = Column(BigInteger, primary_key=True, index=True)
#     storeid = Column(Integer, ForeignKey("store.tid"))
#     DeviceCode = Column(SmallInteger)
#     LogTime = Column(DateTime)
#     Errorcode = Column(Integer)
#     ErrorMessage = Column(String(120))

# class NumCrowd(Base):
#     __tablename__ = "num_crowd"
#     # Giả định recordtime và storeid là khóa chính composite
#     recordtime = Column(DateTime, primary_key=True, index=True)
#     storeid = Column(Integer, ForeignKey("store.tid"), primary_key=True)
#     in_num = Column(Integer, nullable=False)
#     out_num = Column(Integer, nullable=False)














# Định nghĩa các SQLAlchemy models (Store, NumCrowd, ErrLog, Status)

# from sqlalchemy import Column, BigInteger, Boolean, CHAR, DateTime, Integer, NCHAR, SmallInteger, String, ForeignKey
# from sqlalchemy.orm import relationship, declarative_base
# from sqlalchemy.schema import PrimaryKeyConstraint

# # Base class cho tất cả các model
# Base = declarative_base()

# class Store(Base):
#     __tablename__ = 'store'

#     # Ánh xạ các cột trong bảng dbo.store
#     tid = Column(Integer, primary_key=True, index=True)
#     # country = Column(CHAR(20))
#     # area = Column(CHAR(20))
#     # province = Column(CHAR(20))
#     # city = Column(CHAR(20))
#     name = Column(CHAR(80), nullable=False)
#     # address = Column(CHAR(80), nullable=False)
#     # isbranch = Column(CHAR(3))
#     # code = Column(CHAR(32), nullable=False, unique=True)
#     # cameranum = Column(Integer, nullable=False, default=0)
#     # manager = Column(CHAR(20))
#     # managertel = Column(CHAR(20))
#     # lastEditDate = Column(DateTime)
#     # formula = Column(CHAR(64))

#     # Chỉ định schema của bảng
#     __table_args__ = {'schema': 'dbo'}

#     # Định nghĩa mối quan hệ một-nhiều với các bảng khác
#     num_crowds = relationship('NumCrowd', back_populates='store')
#     error_logs = relationship('ErrLog', back_populates='store')
#     statuses = relationship('Status', back_populates='store')

# class NumCrowd(Base):
#     __tablename__ = 'num_crowd'

#     # Ánh xạ các cột trong bảng dbo.ErrLog
#     recordtime = Column(DateTime, nullable=False)
#     in_num = Column(Integer, nullable=False)
#     out_num = Column(Integer, nullable=False)
#     # position = Column(CHAR(30))
#     storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)

#     # Chỉ định schema của bảng
#     __table_args__ = {'schema': 'dbo'}

#     # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
#     store = relationship('Store', back_populates='num_crowds')

#     # Định nghĩa khóa chính phức hợp
#     __table_args__ = (
#         PrimaryKeyConstraint('recordtime', 'storeid'),
#     )

# class ErrLog(Base):
#     __tablename__ = 'ErrLog'

#     # Ánh xạ các cột trong bảng dbo.ErrLog
#     ID = Column(BigInteger, primary_key=True, index=True)
#     storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
#     # DeviceCode = Column(SmallInteger)
#     LogTime = Column(DateTime, nullable=False)
#     Errorcode = Column(Integer)
#     ErrorMessage = Column(NCHAR(120))

#     # Chỉ định schema của bảng
#     __table_args__ = {'schema': 'dbo'}

#     # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
#     store = relationship('Store', back_populates='error_logs')

# class Status(Base):
#     __tablename__ = 'Status'

#     # Ánh xạ các cột trong bảng dbo.ErrLog
#     ID = Column(Integer, primary_key=True, index=True)
#     storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
#     FlashNum = Column(Integer)
#     RamNum = Column(Integer)
#     RC1 = Column(Boolean)
#     RC2 = Column(Boolean)
#     RC3 = Column(Boolean)
#     RC4 = Column(Boolean)
#     RC5 = Column(Boolean)
#     RC6 = Column(Boolean)
#     RC7 = Column(Boolean)
#     RC8 = Column(Boolean)
#     DcID = Column(SmallInteger)
#     FV = Column(NCHAR(20))
#     DcTime = Column(DateTime)
#     DeviceID = Column(SmallInteger)
#     IA = Column(Integer)
#     OA = Column(Integer)
#     S = Column(SmallInteger)
#     T = Column(DateTime)

#     # Chỉ định schema của bảng
#     __table_args__ = {'schema': 'dbo'}

#     # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
#     store = relationship('Store', back_populates='statuses')






# Định nghĩa các SQLAlchemy models (Store, NumCrowd, ErrLog, Status)

from sqlalchemy import Column, BigInteger, Boolean, CHAR, DateTime, Integer, NCHAR, SmallInteger, String, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.schema import PrimaryKeyConstraint

# Base class cho tất cả các model
Base = declarative_base()

class Store(Base):
    __tablename__ = 'store'

    # Ánh xạ các cột trong bảng dbo.store
    tid = Column(Integer, primary_key=True, index=True)
    # country = Column(CHAR(20))
    # area = Column(CHAR(20))
    # province = Column(CHAR(20))
    # city = Column(CHAR(20))
    name = Column(CHAR(80), nullable=False)
    # address = Column(CHAR(80), nullable=False)
    # isbranch = Column(CHAR(3))
    # code = Column(CHAR(32), nullable=False, unique=True)
    # cameranum = Column(Integer, nullable=False, default=0)
    # manager = Column(CHAR(20))
    # managertel = Column(CHAR(20))
    # lastEditDate = Column(DateTime)
    # formula = Column(CHAR(64))

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Định nghĩa mối quan hệ một-nhiều với các bảng khác
    num_crowds = relationship('NumCrowd', back_populates='store')
    error_logs = relationship('ErrLog', back_populates='store')
    statuses = relationship('Status', back_populates='store')

class NumCrowd(Base):
    __tablename__ = 'num_crowd'

    # Ánh xạ các cột trong bảng dbo.ErrLog
    recordtime = Column(DateTime, nullable=False)
    in_num = Column(Integer, nullable=False)
    out_num = Column(Integer, nullable=False)
    # position = Column(CHAR(30))
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
    store = relationship('Store', back_populates='num_crowds')

    # Định nghĩa khóa chính phức hợp
    __table_args__ = (
        PrimaryKeyConstraint('recordtime', 'storeid'),
    )

class ErrLog(Base):
    __tablename__ = 'ErrLog'

    # Ánh xạ các cột trong bảng dbo.ErrLog
    ID = Column(BigInteger, primary_key=True, index=True)
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
    # DeviceCode = Column(SmallInteger)
    LogTime = Column(DateTime, nullable=False)
    Errorcode = Column(Integer)
    ErrorMessage = Column(NCHAR(120))

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
    store = relationship('Store', back_populates='error_logs')

# class Status(Base):
#     __tablename__ = 'Status'

#     # Ánh xạ các cột trong bảng dbo.ErrLog
#     ID = Column(Integer, primary_key=True, index=True)
#     storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
#     FlashNum = Column(Integer)
#     RamNum = Column(Integer)
#     RC1 = Column(Boolean)
#     RC2 = Column(Boolean)
#     RC3 = Column(Boolean)
#     RC4 = Column(Boolean)
#     RC5 = Column(Boolean)
#     RC6 = Column(Boolean)
#     RC7 = Column(Boolean)
#     RC8 = Column(Boolean)
#     DcID = Column(SmallInteger)
#     FV = Column(NCHAR(20))
#     DcTime = Column(DateTime)
#     DeviceID = Column(SmallInteger)
#     IA = Column(Integer)
#     OA = Column(Integer)
#     S = Column(SmallInteger)
#     T = Column(DateTime)

#     # Chỉ định schema của bảng
#     __table_args__ = {'schema': 'dbo'}

#     # Định nghĩa mối quan hệ nhiều-một ngược lại với Store
#     store = relationship('Store', back_populates='statuses')



















from sqlalchemy import Column, ForeignKey, Integer, SmallInteger, BigInteger, CHAR, NCHAR, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import PrimaryKeyConstraint

Base = declarative_base()

class Store(Base):
    __tablename__ = 'store'
    tid = Column(Integer, primary_key = True)
    country = Column(CHAR(20))
    area = Column(CHAR(20))
    province = Column(CHAR(20))
    city = Column(CHAR(20))
    name = Column(CHAR(80), nullable = False)
    address = Column(CHAR(80), nullable = False)
    isbranch = Column(CHAR(3))
    code = Column(CHAR(32), nullable = False)
    cameranum = Column(Integer, nullable = False)
    manager = Column(CHAR(20))
    managertel = Column(CHAR(20))
    lastEditDate = Column(DateTime)
    formula = Column(CHAR(64))

    # def _asdict(self):
    #     return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

class NumCrowd(Base):
    __tablename__ = 'num_crowd'
    recordtime = Column(DateTime)
    in_num = Column(Integer, nullable = False)
    out_num = Column(Integer, nullable = False)
    position = Column(CHAR(30))
    storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)

    store = relationship(Store)
    __table_args__ = (
        PrimaryKeyConstraint(recordtime, in_num, out_num, storeid),
    )

class ErrLog(Base):
    __tablename__ = 'ErrLog'
    ID = Column(BigInteger, primary_key = True)
    storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)
    DeviceCode = Column(SmallInteger)
    LogTime = Column(DateTime)
    Errorcode = Column(Integer)
    ErrorMessage = Column(NCHAR(120))

    store = relationship(Store)

class Status(Base):
    __tablename__ = 'Status'
    ID = Column(Integer, primary_key = True)
    storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)
    FlashNum = Column(Integer)
    RamNum = Column(Integer)
    RC1 = Column(Boolean)
    RC2 = Column(Boolean)
    RC3 = Column(Boolean)
    RC4 = Column(Boolean)
    RC5 = Column(Boolean)
    RC6 = Column(Boolean)
    RC7 = Column(Boolean)
    RC8 = Column(Boolean)
    DcID = Column(SmallInteger)
    FV = Column(NCHAR(20))
    DcTime = Column(DateTime)
    DeviceID = Column(SmallInteger)
    IA = Column(Integer)
    OA = Column(Integer)
    S = Column(SmallInteger)
    T = Column(DateTime)

    store = relationship(Store)















# from sqlalchemy import #, DateTime, Float, ForeignKey
# from sqlalchemy.orm import relationship

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















from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime
from typing import Optional

from .core.database import Base

# --- SQLAlchemy Models (ánh xạ tới bảng trong DB) ---
class Store(Base):
    __tablename__ = 'store'
    __table_args__ = {'schema': 'dbo'}  # Ánh xạ tới schema dbo.store

    tid = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    # crowd_data = relationship('NumCrowd', back_populates='store')
    # error_logs = relationship('ErrLog', back_populates='store')

class NumCrowd(Base):
    __tablename__ = 'num_crowd'
    __table_args__ = {'schema': 'dbo'}

    recordtime = Column(DateTime, primary_key=True, index=True)
    in_num = Column(Integer)
    out_num = Column(Integer)
    storeid = Column(Integer, primary_key=True)

class ErrLog(Base):
    __tablename__ = 'ErrLog'
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
        from_attributes = True

class ErrLogSchema(BaseModel):
    store_name: Optional[str] = 'N/A'
    log_time: datetime
    # error_code: str
    error_message: str

    class Config:
        from_attributes = True

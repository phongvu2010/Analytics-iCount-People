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

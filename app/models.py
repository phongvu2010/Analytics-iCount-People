from sqlalchemy import Column, Integer, String, DateTime, BigInteger, SmallInteger, Boolean, ForeignKey # CHAR, Float, NCHAR
from sqlalchemy.orm import relationship

from .core.database import Base

# =======================================
# SQLAlchemy Model cho Store
# =======================================
class Store(Base):
    """
    SQLAlchemy model for the 'dbo.store' table.
    Represents store information.
    """
    __tablename__ = 'store'

    # Ánh xạ tới schema dbo.store
    __table_args__ = {'schema': 'dbo', 'extend_existing': True}

    # Ánh xạ các cột trong bảng dbo.store
    tid = Column(Integer, primary_key=True, autoincrement=True, comment='Store ID')
    country = Column(String(20))
    area = Column(String(20))
    province = Column(String(20))
    city = Column(String(20))
    name = Column(String(80), nullable=False, comment='Store Name')
    address = Column(String(80), nullable=False)
    isbranch = Column(String(3))
    code = Column(String(32), nullable=False, unique=True, comment='Store Code')
    cameranum = Column(Integer, nullable=False, default=0, comment='Number of cameras')
    manager = Column(String(20))
    managertel = Column(String(20))
    lastEditDate = Column(DateTime)
    formula = Column(String(64))

    # Relationships to other tables (for joining queries)
    error_logs = relationship('ErrLog', back_populates='store')
    crowd_data = relationship('NumCrowd', back_populates='store')
    status_updates = relationship('Status', back_populates='store')

    def __repr__(self):
        return f"<Store(tid={self.tid}, name='{self.name}', code='{self.code}')>"

# =======================================
# SQLAlchemy Model cho ErrLog
# =======================================
class ErrLog(Base):
    """
    SQLAlchemy model for the 'dbo.ErrLog' table.
    Represents device error logs.
    """
    __tablename__ = 'ErrLog'

    # Ánh xạ tới schema dbo.ErrLog
    __table_args__ = {'schema': 'dbo', 'extend_existing': True}

    # Ánh xạ các cột trong bảng dbo.ErrLog
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
    DeviceCode = Column(SmallInteger)
    LogTime = Column(DateTime)
    Errorcode = Column(Integer)
    ErrorMessage = Column(String(120))

    # Relationship to the Store table
    store = relationship('Store', back_populates='error_logs')

    def __repr__(self):
        return f"<ErrLog(ID={self.ID}, storeid={self.storeid}, LogTime='{self.LogTime}')>"

# =======================================
# SQLAlchemy Model cho NumCrowd
# =======================================
class NumCrowd(Base):
    """
    SQLAlchemy model for the 'dbo.num_crowd' table.
    Represents crowd counting data (in/out).
    """
    __tablename__ = 'num_crowd'

    # Ánh xạ tới schema dbo.num_crowd
    __table_args__ = {'schema': 'dbo', 'extend_existing': True}

    # Ánh xạ các cột trong bảng dbo.num_crowd
    recordtime = Column(DateTime, primary_key=True)
    in_num = Column(Integer, nullable=False)
    out_num = Column(Integer, nullable=False)
    position = Column(String(30))
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False, primary_key=True)

    # Relationship to the Store table
    store = relationship('Store', back_populates='crowd_data')

    def __repr__(self):
        return f"<NumCrowd(storeid={self.storeid}, recordtime='{self.recordtime}', in={self.in_num}, out={self.out_num})>"

# =======================================
# SQLAlchemy Model cho Status
# =======================================
class Status(Base):
    """
    SQLAlchemy model for the 'dbo.Status' table.
    Represents device status updates.
    """
    __tablename__ = 'Status'

    # Ánh xạ tới schema dbo.Status
    __table_args__ = {'schema': 'dbo', 'extend_existing': True}

    # Ánh xạ các cột trong bảng dbo.Status
    ID = Column(Integer, primary_key=True, autoincrement=True)
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
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
    FV = Column(String(20), comment='Firmware Version')
    DcTime = Column(DateTime, comment='Device Time')
    DeviceID = Column(SmallInteger)
    IA = Column(Integer)
    OA = Column(Integer)
    S = Column(SmallInteger)
    T = Column(DateTime)

    # Relationship to the Store table
    store = relationship('Store', back_populates='status_updates')

    def __repr__(self):
        return f"<Status(ID={self.ID}, storeid={self.storeid}, DcTime='{self.DcTime}')>"

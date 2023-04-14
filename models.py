
from database import Base

from sqlalchemy import Column, ForeignKey, BigInteger, Float, Date, String, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.schema import PrimaryKeyConstraint

class Camera(Base):
    __tablename__ = 'camera'
    tid = Column(String(15), primary_key = True)
    storeid = Column(String)
    DeviceID = Column(String(10), nullable = False)
    address = Column(Boolean)
    date
    status

class Historical(Base):
    __tablename__ = 'historicals'
    symbol = Column(String, ForeignKey('companies.symbol'))
    date = Column(Date)
    open = Column(Float, nullable = False)
    high = Column(Float, nullable = False)
    low = Column(Float, nullable = False)
    close = Column(Float, nullable = False)
    volume = Column(BigInteger, nullable = False)

    stocks = relationship(Company)
    __table_args__ = (
        PrimaryKeyConstraint(symbol, date),
    )
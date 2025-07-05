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

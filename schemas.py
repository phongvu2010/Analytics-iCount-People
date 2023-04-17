
# class Store(Base):
#     __tablename__ = 'store'
#     tid = Column(Integer, primary_key = True)
#     country = Column(CHAR(20))
#     area = Column(CHAR(20))
#     province = Column(CHAR(20))
#     city = Column(CHAR(20))
#     name = Column(CHAR(80), nullable = False)
#     address = Column(CHAR(80), nullable = False)
#     isbranch = Column(CHAR(3))
#     code = Column(CHAR(32), nullable = False)
#     cameranum = Column(Integer, nullable = False)
#     manager = Column(CHAR(20))
#     managertel = Column(CHAR(20))
#     lastEditDate = Column(DateTime)
#     formula = Column(CHAR(64))

# class Camera(Base):
#     __tablename__ = 'camera'
#     tid = Column(Integer, primary_key = True)
#     storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)
#     DeviceID = Column(SmallInteger)
#     address = Column(CHAR(50), nullable = False)
#     date = Column(DateTime)
#     status = Column(Integer)

#     store = relationship(Store)

# class NumCrowd(Base):
#     __tablename__ = 'num_crowd'
#     recordtime = Column(DateTime)
#     in_num = Column(Integer, nullable = False)
#     out_num = Column(Integer, nullable = False)
#     position = Column(CHAR(30))
#     storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)

#     store = relationship(Store)
#     __table_args__ = (
#         PrimaryKeyConstraint(recordtime, in_num, out_num, storeid),
#     )

# class ErrLog(Base):
#     __tablename__ = 'ErrLog'
#     ID = Column(BigInteger, primary_key = True)
#     storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)
#     DeviceCode = Column(SmallInteger)
#     LogTime = Column(DateTime)
#     Errorcode = Column(Integer)
#     ErrorMessage = Column(NCHAR(120))

#     store = relationship(Store)

# class Status(Base):
#     __tablename__ = 'Status'
#     ID = Column(Integer, primary_key = True)
#     storeid = Column(Integer, ForeignKey('store.tid'), nullable = False)
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

#     store = relationship(Store)

# class Setting(Base):
#     __tablename__ = 'setting'
#     tid = Column(Integer, primary_key = True)
#     companyname = Column(CHAR(50), nullable = False)
#     companyaddress = Column(CHAR(128))
#     companytel = Column(CHAR(20))
#     companyip = Column(CHAR(50), nullable = False)
#     setupdate = Column(DateTime)
#     salername = Column(CHAR(50))
#     saleraddress = Column(CHAR(50))
#     salertel = Column(CHAR(50))

# class User(Base):
#     __tablename__ = 'users'
#     tid = Column(Integer, primary_key = True)
#     name = Column(CHAR(20), nullable = False)
#     password = Column(CHAR(32), nullable = False)
#     realname = Column(CHAR(50), nullable = False)
#     sex = Column(CHAR(6))
#     tel = Column(CHAR(20))
#     address = Column(CHAR(128))
#     country = Column(CHAR(20))
#     area = Column(CHAR(20))
#     province = Column(CHAR(20))
#     city = Column(CHAR(20))
#     storeid = Column(Integer, ForeignKey('store.tid'))

#     store = relationship(Store)

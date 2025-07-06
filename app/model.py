

class Store(Base):
    __tablename__ = 'store'

    # Ánh xạ các cột trong bảng dbo.store
    tid = Column(Integer, primary_key=True)
    country = Column(String(20))
    area = Column(String(20))
    province = Column(String(20))
    city = Column(String(20))
    name = Column(String(80), nullable=False)
    address = Column(String(80), nullable=False)
    isbranch = Column(String(3))
    code = Column(String(32), nullable=False)
    cameranum = Column(Integer, nullable=False)
    manager = Column(String(20))
    managertel = Column(String(20))
    lastEditDate = Column(DateTime)
    formula = Column(String(64))

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Relationships để truy cập các bảng liên quan từ Store
    err_logs = relationship('ErrLog', back_populates='store')
    num_crowds = relationship('NumCrowd', back_populates='store')
    statuses = relationship('Status', back_populates='store')

class ErrLog(Base):
    __tablename__ = 'ErrLog'

    # Ánh xạ các cột trong bảng dbo.ErrLog
    ID = Column(BigInteger, primary_key=True)
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), nullable=False)
    DeviceCode = Column(SmallInteger)
    LogTime = Column(DateTime)
    Errorcode = Column(Integer)
    ErrorMessage = Column(String(120))

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Relationship để truy cập từ Log đến Store
    store = relationship('Store', back_populates='err_logs')

class NumCrowd(Base):
    __tablename__ = 'num_crowd'

    # Ánh xạ các cột trong bảng dbo.ErrLog
    recordtime = Column(DateTime, primary_key=True)
    position = Column(String(30))
    storeid = Column(Integer, ForeignKey('dbo.store.tid'), primary_key=True, nullable=False)

    in_num = Column(Integer, nullable=False)
    out_num = Column(Integer, nullable=False)

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Relationship để truy cập từ Log đến Store
    store = relationship('Store', back_populates='num_crowds')

class Status(Base):
    __tablename__ = 'Status'

    # Ánh xạ các cột trong bảng dbo.ErrLog
    ID = Column(Integer, primary_key=True)
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
    FV = Column(String(20))
    DcTime = Column(DateTime)
    DeviceID = Column(SmallInteger)
    IA = Column(Integer)
    OA = Column(Integer)
    S = Column(SmallInteger)
    T = Column(DateTime)

    # Chỉ định schema của bảng
    __table_args__ = {'schema': 'dbo'}

    # Relationship để truy cập từ Log đến Store
    store = relationship('Store', back_populates='statuses')

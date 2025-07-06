import streamlit as st
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, BigInteger, SmallInteger, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from urllib import parse

@st.cache_resource
def connectDB():
    db_host = st.secrets['DB_HOST']
    db_port = st.secrets['DB_PORT']
    db_name = st.secrets['DB_NAME']
    db_user = st.secrets['DB_USER']
    db_pass = parse.quote_plus(st.secrets['DB_PASS'])
    db_driver = st.secrets['DB_DRIVER']

    DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

    return create_engine(DATABASE_URL, use_setinputsizes = False)

# Tạo engine kết nối tới DB
engine = connectDB()

# 1. CHUỖI KẾT NỐI (CONNECTION STRING)
# Driver 'ODBC Driver 17 for SQL Server' là phổ biến, hãy đảm bảo bạn đã cài đặt nó.
# Nếu bạn sử dụng Windows Authentication, chuỗi kết nối sẽ có dạng khác.
# Ví dụ Windows Auth: f'mssql+pyodbc://{db_host}:{db_port}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
db_host = 'MSSQL'
db_port = 1433
db_name = 'statistic'
db_user = 'sa'
db_pass = parse.quote_plus('Admin@123')
db_driver = 'SQL+Server'

connection_string = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

try:
    # 2. TẠO ENGINE
    # Engine là điểm khởi đầu cho mọi ứng dụng SQLAlchemy.
    engine = create_engine(connection_string)

    # 3. TẠO BASE CLASS CHO CÁC MODEL
    # Đây là lớp cơ sở mà các model của chúng ta sẽ kế thừa.
    Base = declarative_base()

    # 4. TẠO SESSION ĐỂ TRUY VẤN
    Session = sessionmaker(bind=engine)
    session = Session()

    # --- VÍ DỤ TRUY VẤN DỮ LIỆU (READ-ONLY) ---
    print('✅ Kết nối và ánh xạ database thành công!')
    print('\n--- Bắt đầu truy vấn dữ liệu mẫu ---')

    # Ví dụ 1: Lấy 5 cửa hàng đầu tiên từ bảng 'store'
    print('\n[INFO] Lấy 5 cửa hàng đầu tiên:')
    all_stores = session.query(Store).limit(5).all()
    if all_stores:
        for store_instance in all_stores:
            print(f'  - ID: {store_instance.tid}, Tên Cửa Hàng: {store_instance.name}, Mã code: {store_instance.code}')
    else:
        print('  - Không tìm thấy cửa hàng nào.')

    # Ví dụ 2: Lấy 5 log lỗi gần nhất và thông tin cửa hàng tương ứng
    print('\n[INFO] Lấy 5 log lỗi gần nhất và tên cửa hàng:')
    latest_logs = session.query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(5).all()
    # # Câu query join vẫn hoạt động tương tự
    # latest_logs = session.query(ErrLog, Store)\
    #                      .join(Store, ErrLog.storeid == Store.tid)\
    #                      .order_by(ErrLog.LogTime.desc())\
    #                      .limit(10).all()

    if latest_logs:
        for log_instance in latest_logs:
            # Truy cập thông tin store thông qua relationship
            print(f"  - Log Time: {log_instance.LogTime.strftime('%Y-%m-%d %H:%M:%S')}, "
                  f"Cửa hàng: {log_instance.store.name}, "
                  f"Mã lỗi: {log_instance.Errorcode}")
    else:
        print('  - Không tìm thấy log lỗi nào.')

except Exception as e:
    print(f'❌ Đã xảy ra lỗi: {e}')
    print('\n--- GỢI Ý DEBUG ---')
    print('1. Kiểm tra lại chuỗi kết nối (user, password, server, database).')
    print('2. Đảm bảo driver ODBC cho SQL Server đã được cài đặt trên máy của bạn.')
    print('3. Kiểm tra tường lửa hoặc các quy tắc mạng có chặn kết nối đến SQL Server không.')

finally:
    # Luôn đóng session sau khi sử dụng xong để giải phóng tài nguyên.
    if 'session' in locals() and session.is_active:
        session.close()
        print('\n[INFO] Session đã được đóng.')





# import sqlalchemy
# from sqlalchemy import create_engine, MetaData
# from sqlalchemy.orm import sessionmaker, Session
# from sqlalchemy.ext.automap import automap_base

# # Chuỗi kết nối đến MSSQL Server của bạn
# # Thay thế các thông tin <user>, <password>, <server_address>, <database_name>
# # và <driver> cho phù hợp với môi trường của bạn.
# # Ví dụ driver: 'ODBC+Driver+17+for+SQL+Server'
# def connection_string():
#     db_host = st.secrets['DB_HOST']
#     db_port = st.secrets['DB_PORT']
#     db_name = st.secrets['DB_NAME']
#     db_user = st.secrets['DB_USER']
#     db_pass = parse.quote_plus(st.secrets['DB_PASS'])
#     # db_driver = st.secrets['DB_DRIVER']
#     db_driver = 'ODBC+Driver+17+for+SQL+Server'

#     DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

#     return DATABASE_URL

# # Tạo engine kết nối
# engine = create_engine(connection_string())

# # Tạo metadata và automap base
# metadata = MetaData()
# # Tự động ánh xạ (reflect) các bảng từ database
# # Chỉ định các bảng cần ánh xạ để tránh load các bảng không cần thiết
# metadata.reflect(engine, only=['store', 'num_crowd', 'ErrLog'])

# Base = automap_base(metadata=metadata)

# # Chuẩn bị (finalize) các ánh xạ
# # Đây là một bước quan trọng và nên có
# Base.prepare()

# # === BƯỚC GỠ LỖI QUAN TRỌNG ===
# # In ra tất cả tên bảng mà SQLAlchemy đã tìm và ánh xạ thành công
# print("SQLAlchemy đã tìm thấy các bảng sau:", list(Base.classes.keys()))
# # ===============================



# # Tạo các lớp model tương ứng
# Store = Base.classes.store
# NumCrowd = Base.classes.num_crowd
# ErrLog = Base.classes.ErrLog

# # Tạo một session factory để tương tác với database
# # Chúng ta có thể tạo một session ở chế độ "read-only" bằng cách tùy chỉnh lớp Session
# class ReadOnlySession(Session):
#     def flush(self):
#         """Ghi đè phương thức flush để ngăn chặn mọi thay đổi."""
#         # Ném ra một exception hoặc đơn giản là không làm gì cả
#         raise Exception("Session này ở chế độ chỉ đọc!")

# # Tạo session factory với lớp ReadOnlySession
# ReadOnlySessionFactory = sessionmaker(bind=engine, class_=ReadOnlySession)






# from sqlalchemy import create_engine, MetaData
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.ext.declarative import declarative_base


# # Tạo metadata và automap base
# metadata = MetaData()
# Base = automap_base(metadata=metadata)

# # Tự động ánh xạ (reflect) các bảng từ database
# # Chỉ định các bảng cần ánh xạ để tránh load các bảng không cần thiết
# metadata.reflect(engine, only = ['store', 'num_crowd', 'ErrLog'])

# # Tạo các lớp model tương ứng
# Store = Base.classes.store
# NumCrowd = Base.classes.num_crowd
# ErrLog = Base.classes.ErrLog

# # Tạo một session factory để tương tác với database
# # Chúng ta có thể tạo một session ở chế độ "read-only" bằng cách tùy chỉnh lớp Session
# class ReadOnlySession(Session):
#     def flush(self):
#         """ Ghi đè phương thức flush để ngăn chặn mọi thay đổi. """
#         # Ném ra một exception hoặc đơn giản là không làm gì cả
#         raise Exception('Session này ở chế độ chỉ đọc!')

# # Tạo session factory với lớp ReadOnlySession
# ReadOnlySessionFactory = sessionmaker(bind=engine, class_=ReadOnlySession)

# # Initialize connection - Dependency để inject session vào mỗi request
# @st.cache_resource
# def getSession():
#     db = ReadOnlySessionFactory()
#     try:
#         yield db
#     finally:
#         db.close()










# # Tạo một phiên (session) để tương tác với DB
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Base class cho các ORM models
# Base = declarative_base()


# import pandas as pd

# from datetime import date
# from models import Store, NumCrowd, ErrLog, Status
# from sqlalchemy import extract

# @st.cache_data(ttl = 86400, show_spinner = False)
# def dbStore():
#     query = getSession().query(Store)

#     return pd.read_sql(sql = query.statement, con = engine)
#     # return pd.DataFrame([r._asdict() for r in results])

# @st.cache_data(ttl = 900, show_spinner = False)
# def dbNumCrowd(year = None):
#     query = getSession().query(NumCrowd)
#     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbErrLog():
#     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbStatus():
#     query = getSession().query(Status)

#     return pd.read_sql(sql = query.statement, con = engine)

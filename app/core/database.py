# from sqlalchemy import create_engine
# # from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# # from decouple import config

# from .config import settings

# # Chuỗi kết nối đến CSDL MSSQL
# DATABASE_URL = settings.SQLALCHEMY_DATABASE_URI

# # Tạo SQLAlchemy engine kết nối tới DB
# engine = create_engine(DATABASE_URL)

# # # Base class cho các ORM models
# # Base = declarative_base()

# # Tạo một lớp SessionLocal, mỗi instance của lớp này sẽ là một session CSDL
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Hàm dependency để lấy session trong mỗi request
# # và đảm bảo session được đóng lại sau khi request hoàn tất.
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()










# # import streamlit as st
# # from urllib import parse

# # try:
# #     # 2. TẠO ENGINE
# #     # Engine là điểm khởi đầu cho mọi ứng dụng SQLAlchemy.
# #     engine = create_engine(connection_string)

# #     # 3. TẠO BASE CLASS CHO CÁC MODEL
# #     # Đây là lớp cơ sở mà các model của chúng ta sẽ kế thừa.
# #     Base = declarative_base()

# #     # 4. TẠO SESSION ĐỂ TRUY VẤN
# #     Session = sessionmaker(bind=engine)
# #     session = Session()

# #     # --- VÍ DỤ TRUY VẤN DỮ LIỆU (READ-ONLY) ---
# #     print('✅ Kết nối và ánh xạ database thành công!')
# #     print('\n--- Bắt đầu truy vấn dữ liệu mẫu ---')

# #     # Ví dụ 1: Lấy 5 cửa hàng đầu tiên từ bảng 'store'
# #     print('\n[INFO] Lấy 5 cửa hàng đầu tiên:')
# #     all_stores = session.query(Store).limit(5).all()
# #     if all_stores:
# #         for store_instance in all_stores:
# #             print(f'  - ID: {store_instance.tid}, Tên Cửa Hàng: {store_instance.name}, Mã code: {store_instance.code}')
# #     else:
# #         print('  - Không tìm thấy cửa hàng nào.')

# #     # Ví dụ 2: Lấy 5 log lỗi gần nhất và thông tin cửa hàng tương ứng
# #     print('\n[INFO] Lấy 5 log lỗi gần nhất và tên cửa hàng:')
# #     latest_logs = session.query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(5).all()
# #     # # Câu query join vẫn hoạt động tương tự
# #     # latest_logs = session.query(ErrLog, Store)\
# #     #                      .join(Store, ErrLog.storeid == Store.tid)\
# #     #                      .order_by(ErrLog.LogTime.desc())\
# #     #                      .limit(10).all()

# #     if latest_logs:
# #         for log_instance in latest_logs:
# #             # Truy cập thông tin store thông qua relationship
# #             print(f"  - Log Time: {log_instance.LogTime.strftime('%Y-%m-%d %H:%M:%S')}, "
# #                   f"Cửa hàng: {log_instance.store.name}, "
# #                   f"Mã lỗi: {log_instance.Errorcode}")
# #     else:
# #         print('  - Không tìm thấy log lỗi nào.')

# # except Exception as e:
# #     print(f'❌ Đã xảy ra lỗi: {e}')
# #     print('\n--- GỢI Ý DEBUG ---')
# #     print('1. Kiểm tra lại chuỗi kết nối (user, password, server, database).')
# #     print('2. Đảm bảo driver ODBC cho SQL Server đã được cài đặt trên máy của bạn.')
# #     print('3. Kiểm tra tường lửa hoặc các quy tắc mạng có chặn kết nối đến SQL Server không.')

# # finally:
# #     # Luôn đóng session sau khi sử dụng xong để giải phóng tài nguyên.
# #     if 'session' in locals() and session.is_active:
# #         session.close()
# #         print('\n[INFO] Session đã được đóng.')




# # import pandas as pd
# # import sqlalchemy
# # from datetime import date
# # from sqlalchemy import create_engine, extract, MetaData
# # from sqlalchemy.ext.automap import automap_base

# # from sqlalchemy.orm import sessionmaker, Session
# # from models import Store, NumCrowd, ErrLog, Status

# # @st.cache_data(ttl = 86400, show_spinner = False)
# # def dbStore():
# #     query = getSession().query(Store)

# #     return pd.read_sql(sql = query.statement, con = engine)
# #     # return pd.DataFrame([r._asdict() for r in results])

# # @st.cache_data(ttl = 900, show_spinner = False)
# # def dbNumCrowd(year = None):
# #     query = getSession().query(NumCrowd)
# #     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbErrLog():
# #     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbStatus():
# #     query = getSession().query(Status)

# #     return pd.read_sql(sql = query.statement, con = engine)





# # import pandas as pd
# # import streamlit as st

# # from datetime import date
# # from models import Store, NumCrowd, ErrLog, Status
# # from sqlalchemy import create_engine, extract
# # from sqlalchemy.orm import sessionmaker
# # from urllib import parse

# # @st.cache_resource
# # def connectDB():
# #     env = st.secrets['development']

# #     db_host = env['DB_HOST']
# #     db_port = parse.quote_plus(str(env['DB_PORT']))
# #     db_name = env['DB_NAME']
# #     db_user = parse.quote_plus(env['DB_USER'])
# #     db_pass = parse.quote_plus(env['DB_PASS'])

# #     DATABASE_URL = f'mssql+pyodbc://{ db_user }:{ db_pass }@{ db_host }:{ db_port }/{ db_name }?driver=SQL Server'

# #     return create_engine(DATABASE_URL)

# # engine = connectDB()
# # Session = sessionmaker(autocommit = False, autoflush = False, bind = engine)

# # # Initialize connection.
# # @st.cache_resource
# # def getSession():
# #     session = Session()
# #     try:
# #         return session
# #     finally:
# #         session.close()

# # @st.cache_data(ttl = 86400, show_spinner = False)
# # def dbStore():
# #     query = getSession().query(Store)

# #     return pd.read_sql(sql = query.statement, con = engine)
# #     # return pd.DataFrame([r._asdict() for r in results])

# # @st.cache_data(ttl = 900, show_spinner = False)
# # def dbNumCrowd(year = None):
# #     query = getSession().query(NumCrowd)
# #     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbErrLog():
# #     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbStatus():
# #     query = getSession().query(Status)

# #     return pd.read_sql(sql = query.statement, con = engine)




# # import pandas as pd
# # import streamlit as st

# # from datetime import date
# # from models import Store, NumCrowd, ErrLog, Status
# # from sqlalchemy import create_engine, extract
# # from sqlalchemy.orm import sessionmaker
# # from urllib import parse

# # @st.cache_resource
# # def connectDB():
# #     env = st.secrets['development']

# #     db_host = env['DB_HOST']
# #     db_port = parse.quote_plus(str(env['DB_PORT']))
# #     db_name = env['DB_NAME']
# #     db_user = parse.quote_plus(env['DB_USER'])
# #     db_pass = parse.quote_plus(env['DB_PASS'])

# #     DATABASE_URL = f'mssql+pyodbc://{ db_user }:{ db_pass }@{ db_host }:{ db_port }/{ db_name }?driver=SQL Server'

# #     return create_engine(DATABASE_URL)

# # engine = connectDB()
# # Session = sessionmaker(autocommit = False, autoflush = False, bind = engine)

# # # Initialize connection.
# # @st.cache_resource
# # def getSession():
# #     session = Session()
# #     try:
# #         return session
# #     finally:
# #         session.close()

# # @st.cache_data(ttl = 86400, show_spinner = False)
# # def dbStore():
# #     query = getSession().query(Store)

# #     return pd.read_sql(sql = query.statement, con = engine)
# #     # return pd.DataFrame([r._asdict() for r in results])

# # @st.cache_data(ttl = 900, show_spinner = False)
# # def dbNumCrowd(year = None):
# #     query = getSession().query(NumCrowd)
# #     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbErrLog():
# #     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

# #     return pd.read_sql(sql = query.statement, con = engine)

# # @st.cache_data(ttl = 3600, show_spinner = False)
# # def dbStatus():
# #     query = getSession().query(Status)

# #     return pd.read_sql(sql = query.statement, con = engine)




# # import streamlit as st
# # from sqlalchemy import create_engine
# # from sqlalchemy.orm import sessionmaker
# # from urllib import parse

# # @st.cache_resource
# # def connect_db():
# #     db_host = st.secrets['DB_HOST']
# #     db_port = st.secrets['DB_PORT']
# #     db_name = st.secrets['DB_NAME']
# #     db_user = st.secrets['DB_USER']
# #     db_pass = parse.quote_plus(st.secrets['DB_PASS'])
# #     db_driver = st.secrets['DB_DRIVER']
# #     DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

# #     return create_engine(DATABASE_URL)

# # # Tạo engine kết nối tới DB
# # engine = connect_db()

# # # Tạo một phiên (session) để tương tác với DB
# # SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # # # Base class cho các ORM models
# # # Base = declarative_base()

# # # Initialize connection - Dependency để inject session vào mỗi request
# # @st.cache_resource
# # def get_db():
# #     db = SessionLocal()
# #     try:
# #         return db
# #     finally:
# #         db.close()
# #         print('\n[INFO] Session đã được đóng.')

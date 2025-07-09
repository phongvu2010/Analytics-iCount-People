import pyodbc
import pandas as pd
from .config import settings

def get_db_connection_string() -> str:
    """
    Tạo chuỗi kết nối tới MSSQL từ các biến môi trường.
    """
    # return (
    #     f"DRIVER={settings.DB_DRIVER};"
    #     f"SERVER={settings.DB_HOST};"
    #     f"DATABASE={settings.DB_DATABASE};"
    #     f"UID={settings.DB_USERNAME};"
    #     f"PWD={settings.DB_PASSWORD};"
    #     f"TrustServerCertificate=yes;" # Thêm dòng này nếu bạn dùng self-signed certificate
    # )
    return settings.SQLALCHEMY_DATABASE_URI

def get_db_connection():
    """
    Tạo và trả về một đối tượng kết nối pyodbc.
    Trả về None nếu kết nối thất bại.
    """
    try:
        conn_str = get_db_connection_string()
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Lỗi kết nối Database: {sqlstate}")
        print(ex)
        return None

def fetch_data_as_dataframe(query: str, params=None) -> pd.DataFrame:
    """
    Thực thi một câu lệnh SQL và trả về kết quả dưới dạng Pandas DataFrame.
    """
    conn = get_db_connection()
    if conn:
        try:
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Lỗi khi thực thi query: {e}")
            return pd.DataFrame() # Trả về DataFrame rỗng nếu có lỗi
        finally:
            conn.close()
    return pd.DataFrame()




# from sqlalchemy import create_engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

# from .config import settings

# # Chuỗi kết nối đến CSDL MSSQL
# DATABASE_URL = settings.SQLALCHEMY_DATABASE_URI
# # DATABASE_URL = 'sqlite:///./database.db'

# # Tạo SQLAlchemy engine kết nối tới DB
# engine = create_engine(
#     DATABASE_URL,
#     # Nếu dùng SQLite, cần dòng connect_args này để tương thích với FastAPI
#     connect_args={'check_same_thread': False} if 'sqlite' in DATABASE_URL else {}
# )

# # Tạo một lớp SessionLocal, mỗi instance của lớp này sẽ là một session CSDL
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Base class cho các ORM models
# Base = declarative_base()

# # Dependency để lấy DB session trong mỗi request và đảm bảo session
# # được đóng lại sau khi request hoàn tất để giải phóng tài nguyên.
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# Gộp logic kết nối và session CSDL

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import settings

# Chuỗi kết nối đến CSDL MSSQL
# Ví dụ: 'mssql+pyodbc://user:password@host:port/dbname?driver=ODBC+Driver+17+for+SQL+Server'
DATABASE_URL = settings.DATABASE_URL

# Tạo SQLAlchemy engine kết nối tới DB
engine = create_engine(DATABASE_URL)

# Tạo một lớp SessionLocal, mỗi instance của lớp này sẽ là một session CSDL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Hàm dependency để cung cấp một session CSDL cho mỗi request
# và đảm bảo session được đóng lại sau khi request hoàn tất.
def get_db():
    """
    Dependency function to get a database session.
    Yields:
        Session: The database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

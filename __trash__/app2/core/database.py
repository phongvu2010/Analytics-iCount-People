from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .config import settings

# Chuỗi kết nối đến CSDL MSSQL
DATABASE_URL = settings.SQLALCHEMY_DATABASE_URI
# DATABASE_URL = 'sqlite:///./database.db'

# Tạo SQLAlchemy engine kết nối tới DB
engine = create_engine(
    DATABASE_URL,
    # Nếu dùng SQLite, cần dòng connect_args này để tương thích với FastAPI
    connect_args={'check_same_thread': False} if 'sqlite' in DATABASE_URL else {}
)

# Tạo một lớp SessionLocal, mỗi instance của lớp này sẽ là một session CSDL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class cho các ORM models
Base = declarative_base()

# Dependency để lấy DB session trong mỗi request và đảm bảo session
# được đóng lại sau khi request hoàn tất để giải phóng tài nguyên.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

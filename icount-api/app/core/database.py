from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .config import settings

# Tạo engine kết nối tới DB
engine = create_engine(settings.SQLALCHEMY_DATABASE_URL)

# Tạo một phiên (session) để tương tác với DB
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class cho các ORM models
Base = declarative_base()

# Dependency để cung cấp DB session cho mỗi request
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

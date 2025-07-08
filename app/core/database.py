# # === FILENAME: database.py ===
# # Module thiết lập kết nối và session tới cơ sở dữ liệu.

# from sqlalchemy import create_engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# from decouple import config

# DATABASE_URL = config('DATABASE_URL')

# engine = create_engine(
#     DATABASE_URL, 
#     # Nếu dùng SQLite, cần dòng connect_args này để tương thích với FastAPI
#     connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
# )

# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base = declarative_base()

# # Dependency để lấy DB session trong mỗi request
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

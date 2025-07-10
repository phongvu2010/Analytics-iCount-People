import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from .config import settings

# Tạo SQLAlchemy engine
engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    echo=False,             # False trong môi trường production để tránh in ra quá nhiều log
    pool_pre_ping=True      # 
)

# Tạo một lớp SessionLocal được cấu hình
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency function cho FastAPI để cung cấp một session DB cho mỗi request.
    Sử dụng 'yield' để đảm bảo session được đóng lại sau khi request hoàn tất,
    kể cả khi có lỗi xảy ra.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def execute_query_as_dataframe(query: str, db: Session, params: dict = None) -> pd.DataFrame:
    """
    Thực thi một câu lệnh SQL sử dụng session SQLAlchemy và trả về kết quả
    dưới dạng Pandas DataFrame.

    Args:
        query (str): Câu lệnh SQL với named parameters (ví dụ: :param_name).
        db (Session): Session SQLAlchemy được inject từ dependency.
        params (dict, optional): Dictionary chứa các tham số cho câu lệnh SQL.

    Returns:
        pd.DataFrame: DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
    """
    try:
        # Sử dụng connection của session để thực thi với pandas
        # db.connection() trả về DBAPI connection
        df = pd.read_sql_query(sql=text(query), con=db.connection(), params=params)
        return df
    except Exception as e:
        print(f'Lỗi khi thực thi query với SQLAlchemy: {e}')
        # Trong production, bạn nên log lỗi này vào một file hoặc hệ thống logging
        return pd.DataFrame()

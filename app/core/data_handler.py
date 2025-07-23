# data_handler.py
import duckdb
import logging
import pandas as pd

from ..core.config import settings # Import settings

# Kết nối DuckDB sẽ phụ thuộc vào cấu hình
def get_duckdb_connection_for_query():
    """Tạo và trả về một kết nối DuckDB dựa trên cấu hình DATABASE_TYPE.

    Nếu là 'duckdb_file', sẽ kết nối đến file đó.
    Nếu là 'parquet_folder', sẽ dùng kết nối in-memory.
    """
    if settings.DATABASE_TYPE == 'duckdb_file':
        if not settings.DUCKDB_FILE_PATH:
            raise ValueError("DUCKDB_FILE_PATH phải được cung cấp khi DATABASE_TYPE là 'duckdb_file'")
        return duckdb.connect(database=settings.DUCKDB_FILE_PATH, read_only=True) # Chỉ đọc để đảm bảo an toàn
    else: # Mặc định hoặc 'parquet_folder'
        return duckdb.connect(database=':memory:', read_only=False) # In-memory cho parquet, cần write_only=False để đọc

def query_dataframe(query: str, params: list = None) -> pd.DataFrame:
    """Thực thi một câu lệnh SQL trên dữ liệu (Parquet hoặc DuckDB file) bằng DuckDB.

    Hàm này mở một kết nối DuckDB, thực thi truy vấn, và đóng kết nối
    để giải phóng tài nguyên.
    """
    try:
        with get_duckdb_connection_for_query() as con: # Sử dụng context manager
            return con.execute(query, parameters=params).df()
    except Exception as e:
        logging.error(f'Lỗi khi thực thi query với DuckDB: {e}\nQuery: {query}\nParams: {params}')
        return pd.DataFrame()

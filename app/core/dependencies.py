"""
Module này định nghĩa các dependency cho API.

Dependency Injection là một tính năng mạnh mẽ của FastAPI, cho phép
tách biệt và tái sử dụng các logic chung (như kết nối database,
xác thực người dùng).
"""
import duckdb
import logging

from duckdb import DuckDBPyConnection
from fastapi import HTTPException
from typing import Iterator

from .config import settings

logger = logging.getLogger(__name__)

def get_db_connection() -> Iterator[DuckDBPyConnection]:
    """
    Dependency để cung cấp kết nối đến DuckDB cho mỗi request.

    Sử dụng một generator với try...finally để đảm bảo kết nối
    luôn được đóng lại sau khi request hoàn tất, kể cả khi có lỗi.

    Yields:
        DuckDBPyConnection: Đối tượng kết nối DuckDB.
    """
    conn = None
    try:
        # Kết nối tới file DuckDB ở chế độ chỉ đọc (read_only=True)
        # vì API chỉ có nhiệm vụ đọc dữ liệu, không ghi.
        db_path = str(settings.DUCKDB_PATH.resolve())
        logger.debug(f"Mở kết nối tới DuckDB tại: {db_path}")
        # Kết nối ở chế độ READ_ONLY để đảm bảo an toàn, API chỉ đọc dữ liệu
        conn = duckdb.connect(database=db_path, read_only=True)
        yield conn

    except duckdb.Error as e:
        logger.critical(f"Không thể kết nối tới DuckDB: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail='Lỗi hệ thống: Không thể kết nối cơ sở dữ liệu.'
        )

    finally:
        if conn:
            conn.close()
            logger.debug('Kết nối DuckDB đã được đóng.')




# import pandas as pd

# def get_duckdb_connection():
#     """
#     Tạo và trả về một kết nối DuckDB in-memory.

#     Sử dụng kết nối ':memory:' để đạt hiệu năng cao nhất cho các tác vụ đọc.
#     Mỗi lời gọi sẽ tạo một kết nối mới để đảm bảo thread-safety khi
#     chạy các truy vấn song song.
#     """
#     return duckdb.connect(database=':memory:', read_only=False)

# def query_parquet_as_dataframe(query: str, params: list = None) -> pd.DataFrame:
#     """
#     Thực thi một câu lệnh SQL trên các tệp Parquet bằng DuckDB.

#     Hàm này mở một kết nối DuckDB, thực thi truy vấn, và đóng kết nối
#     để giải phóng tài nguyên.

#     Args:
#         query: Câu lệnh SQL để thực thi.
#         params: Danh sách các tham số để truyền vào câu lệnh SQL một cách an toàn.

#     Returns:
#         Một DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
#     """
#     con = get_duckdb_connection()
#     try:
#         # Thực thi câu lệnh và trả về kết quả dưới dạng Pandas DataFrame
#         return con.execute(query, parameters=params).df()

#     except Exception as e:
#         logger.error(f"Lỗi khi thực thi query với DuckDB: {e}\nQuery: {query}\nParams: {params}")
#         return pd.DataFrame()

#     finally:
#         # Đảm bảo kết nối luôn được đóng sau khi sử dụng.
#         con.close()

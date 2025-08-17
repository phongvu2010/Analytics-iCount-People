"""
Module này định nghĩa các dependency cho API.

Dependency Injection là một tính năng mạnh mẽ của FastAPI, cho phép
tách biệt và tái sử dụng các logic chung (như kết nối database,
xác thực người dùng).
"""
import duckdb
import logging
import pandas as pd

from contextlib import contextmanager
from duckdb import DuckDBPyConnection
from typing import Iterator

from .config import settings

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection() -> Iterator[DuckDBPyConnection]:
    """
    Context manager để quản lý vòng đời kết nối DuckDB.

    Nó sẽ mở một kết nối khi được gọi và tự động đóng lại
    khi khối lệnh kết thúc, kể cả khi có lỗi xảy ra.
    Sử dụng chế độ read_only=True vì API chỉ có nhiệm vụ đọc dữ liệu.
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
        # Ném lại lỗi để FastAPI có thể bắt và trả về lỗi 500
        raise

    finally:
        if conn:
            conn.close()
            logger.debug('Kết nối DuckDB đã được đóng.')

def query_db_to_df(query: str, params: list = None) -> pd.DataFrame:
    """
    Hàm tiện ích để thực thi một câu lệnh SQL và trả về Pandas DataFrame.
    Hàm này sẽ quản lý kết nối của riêng nó, phù hợp cho các tác vụ
    đơn lẻ, không cần truyền đối tượng kết nối đi nhiều nơi.
    """
    try:
        with get_db_connection() as conn:
            return conn.execute(query, parameters=params).df()
    except Exception:
        # Lỗi đã được log trong get_db_connection, ở đây chỉ trả về df rỗng
        return pd.DataFrame()

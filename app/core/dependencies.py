"""
Module này định nghĩa các dependency cho API.

Dependency Injection là một tính năng mạnh mẽ của FastAPI, cho phép
tách biệt và tái sử dụng các logic chung (như kết nối database,
xác thực người dùng).
"""
import duckdb
import logging
import pandas as pd

from .config import settings

logger = logging.getLogger(__name__)

def get_db_connection(query: str, params: list = None) -> pd.DataFrame:
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
        return conn.execute(query, parameters=params).df()

    except duckdb.Error as e:
        logger.critical(f"Không thể kết nối tới DuckDB: {e}", exc_info=True)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
            logger.debug('Kết nối DuckDB đã được đóng.')

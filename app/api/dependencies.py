"""
Module này định nghĩa các dependency cho API.

Dependency Injection là một tính năng mạnh mẽ của FastAPI, cho phép
tách biệt và tái sử dụng các logic chung (như kết nối database,
xác thực người dùng).
"""
import duckdb
import logging

from typing import Iterator

from ..core.config import etl_settings

logger = logging.getLogger(__name__)

def get_db_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """
    FastAPI dependency để tạo và cung cấp một kết nối DuckDB.

    Hàm này sử dụng `yield` để tạo ra một context manager:
    -   Trước `yield`: Mở kết nối database.
    -   `yield`: Giao kết nối cho endpoint sử dụng.
    -   Sau `yield` (trong khối finally): Đóng kết nối để giải phóng tài nguyên.

    Returns:
        Một iterator chứa kết nối DuckDB đang hoạt động.
    """
    db_conn = None
    try:
        # Kết nối tới file DuckDB ở chế độ chỉ đọc (read_only=True)
        # vì API chỉ có nhiệm vụ đọc dữ liệu, không ghi.
        db_path = str(etl_settings.DUCKDB_PATH.resolve())
        logger.debug(f"API: Đang mở kết nối read-only tới DuckDB tại {db_path}")
        db_conn = duckdb.connect(database=db_path, read_only=True)
        yield db_conn
    except duckdb.Error as e:
        logger.error(f"API: Lỗi kết nối DuckDB: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
            logger.debug('API: Kết nối DuckDB đã được đóng.')

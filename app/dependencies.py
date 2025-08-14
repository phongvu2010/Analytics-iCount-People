import duckdb
import logging

from fastapi import HTTPException
from typing import Iterator

from .core.config import settings

logger = logging.getLogger(__name__)

def get_db_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Dependency để cung cấp kết nối đến DuckDB cho mỗi request.

    Sử dụng một generator với try...finally để đảm bảo kết nối
    luôn được đóng lại sau khi request hoàn tất, kể cả khi có lỗi.

    Yields:
        duckdb.DuckDBPyConnection: Đối tượng kết nối DuckDB.
    """
    db_path = str(settings.DUCKDB_PATH.resolve())
    conn = None
    try:
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

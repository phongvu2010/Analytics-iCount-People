"""
Định nghĩa các dependency cho API, giúp tái sử dụng logic chung.

Dependency Injection là một tính năng cốt lõi của FastAPI, cho phép tách biệt
và tái sử dụng các thành phần như kết nối database, xác thực người dùng,
giúp mã nguồn trở nên module hóa và dễ kiểm thử hơn.
"""
import duckdb
import logging
import pandas as pd

from contextlib import contextmanager
from duckdb import DuckDBPyConnection, Error as DuckDBError
from typing import Iterator

from .config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection() -> Iterator[DuckDBPyConnection]:
    """
    Context manager để quản lý vòng đời của một kết nối đến DuckDB.

    Nó sẽ mở một kết nối khi được gọi và tự động đóng lại khi khối lệnh
    kết thúc, kể cả khi có lỗi xảy ra. Kết nối được mở ở chế độ chỉ đọc
    (read-only) để đảm bảo an toàn cho dữ liệu trong môi trường API.

    Yields:
        Một đối tượng kết nối DuckDB đang hoạt động.

    Raises:
        Error: Nếu không thể kết nối tới file database.
    """
    conn = None
    try:
        db_path = str(settings.DUCKDB_PATH.resolve())
        logger.debug(f"Đang mở kết nối tới DuckDB (read-only): {db_path}")

        # Kết nối ở chế độ READ_ONLY để đảm bảo an toàn, API chỉ đọc dữ liệu
        conn = duckdb.connect(database=db_path, read_only=True)
        yield conn

    except DuckDBError as e:
        logger.critical(f"Không thể kết nối tới DuckDB: {e}", exc_info=True)
        raise  # Ném lại lỗi để FastAPI xử lý và trả về lỗi 500.

    finally:
        if conn:
            conn.close()
            logger.debug('Kết nối DuckDB đã được đóng.')


def query_db_to_df(query: str, params: list = None) -> pd.DataFrame:
    """
    Hàm tiện ích để thực thi một câu lệnh SQL và trả về kết quả dưới dạng DataFrame.

    Hàm này tự quản lý kết nối, phù hợp cho các tác vụ đơn lẻ không cần
    truyền đối tượng kết nối đi nhiều nơi.

    Args:
        query: Câu lệnh SQL cần thực thi.
        params: Danh sách các tham số cho câu lệnh SQL để chống SQL injection.

    Returns:
        Một Pandas DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
    """
    try:
        with get_db_connection() as conn:
            return conn.execute(query, parameters=params).df()
    except Exception:
        # Lỗi đã được log trong get_db_connection, ở đây chỉ cần trả về
        # một DataFrame rỗng để tránh làm sập ứng dụng.
        return pd.DataFrame()

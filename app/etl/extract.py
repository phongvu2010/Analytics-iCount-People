"""
Module này xử lý giai đoạn 'E' (Extract) của pipeline ETL.

Chức năng chính là kết nối tới nguồn dữ liệu (MS SQL Server) và
trích xuất dữ liệu dựa trên cấu hình được cung cấp.
"""
import logging
import pandas as pd

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterator

from app.core.config import etl_settings, TableConfig

logger = logging.getLogger(__name__)

def from_sql_server(sql_engine: Engine, config: TableConfig, last_timestamp: str) -> Iterator[pd.DataFrame]:
    """
    Trích xuất dữ liệu từ một bảng trong MS SQL Server theo từng khối (chunk).

    Hàm này xây dựng câu lệnh SQL để lấy tất cả dữ liệu hoặc chỉ dữ liệu mới
    dựa trên cấu hình `incremental` và `timestamp_col`.

    Args:
        sql_engine (Engine): SQLAlchemy engine đã kết nối tới SQL Server.
        config (TableConfig): Cấu hình cho bảng cần trích xuất.
        last_timestamp (str): Giá trị high-water mark (lần chạy thành công cuối).
                              Dùng cho incremental load.

    Returns:
        Iterator[pd.DataFrame]: Một iterator cho phép xử lý dữ liệu theo từng khối
                                mà không cần tải toàn bộ vào bộ nhớ.

    Raises:
        SQLAlchemyError: Nếu có lỗi xảy ra trong quá trình thực thi câu lệnh SQL.
    """
    # Xây dựng câu lệnh SELECT cơ bản
    # Lưu ý: f-string ở đây an toàn vì tên bảng/cột được kiểm soát nội bộ qua config
    query = f"SELECT * FROM {config.source_table}"
    params = {}

    # Nếu là incremental load, thêm mệnh đề WHERE và ORDER BY
    if config.incremental and config.timestamp_col:
        query += f" WHERE {config.timestamp_col} > :last_ts ORDER BY {config.timestamp_col}"
        params = {"last_ts": last_timestamp}
        logger.info(f"Trích xuất incremental từ '{config.source_table}' với high-water-mark > '{last_timestamp}'.")
    else:
        logger.info(f"Trích xuất full-load từ '{config.source_table}'.")

    try:
        # Sử dụng pd.read_sql với chunksize để trả về một iterator
        # Dùng `text()` và `params` của SQLAlchemy đểป้องกัน SQL Injection cho các giá trị đầu vào
        return pd.read_sql(
            sql=text(query),
            con=sql_engine,
            params=params,
            chunksize=etl_settings.ETL_CHUNK_SIZE
        )
    except SQLAlchemyError as e:
        logger.error(f"Lỗi SQL khi trích xuất từ bảng {config.source_table}: {e}")
        raise

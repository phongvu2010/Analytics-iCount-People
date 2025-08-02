import logging
import pandas as pd

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterator

from app.core.config import etl_settings, TableConfig

logger = logging.getLogger(__name__)

def from_sql_server(sql_engine: Engine, config: TableConfig, last_timestamp: str) -> Iterator[pd.DataFrame]:
    query = f"SELECT * FROM {config.source_table}"
    params = {}

    if config.incremental and config.timestamp_col:
        query += f" WHERE {config.timestamp_col} > :last_ts ORDER BY {config.timestamp_col}"
        params = {"last_ts": last_timestamp}

    logger.info(f"Đang trích xuất dữ liệu từ '{config.source_table}' với high-water-mark > '{last_timestamp}'.")

    try:
        return pd.read_sql(text(query), sql_engine, params=params, chunksize=etl_settings.ETL_CHUNK_SIZE)
    except SQLAlchemyError as e:
        logger.error(f"Lỗi SQL khi trích xuất từ bảng {config.source_table}: {e}")
        raise

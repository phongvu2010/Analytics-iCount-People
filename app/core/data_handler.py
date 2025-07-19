import duckdb
import logging
import pandas as pd

from functools import lru_cache

from .config import settings
from ..utils.logger import setup_logging

# Cache kết nối để không phải tạo lại liên tục
@lru_cache(maxsize=1)
def get_duckdb_connection():
    """
    Tạo và trả về một kết nối DuckDB duy nhất cho ứng dụng.
    Sử dụng lru_cache để đảm bảo kết nối được tái sử dụng, tăng hiệu suất.
    """
    # return duckdb.connect(database=':memory:', read_only=False)
    return duckdb.connect(database=settings.DUCKDB_PATH, read_only=True)

def query_parquet_as_dataframe(query: str, params: list=None) -> pd.DataFrame:
    """
    Thực thi một câu lệnh SQL trên các file Parquet bằng DuckDB.

    Args:
        query (str): Câu lệnh SQL để thực thi. DuckDB sẽ chạy câu lệnh này
                     trực tiếp trên các file được chỉ định trong query.

    Returns:
        pd.DataFrame: DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
    """
    con = get_duckdb_connection()
    try:
        # Thực thi câu lệnh và trả về kết quả dưới dạng Pandas DataFrame
        # Truyền tham số vào hàm execute nếu có
        return con.execute(query, parameters=params).df()
    except Exception as e:
        setup_logging('database_duckdb')
        logging.error(f'Lỗi khi thực thi query với DuckDB: {e}\n')
        return pd.DataFrame()

import duckdb
import pandas as pd
from functools import lru_cache

# Đường dẫn tới thư mục chứa dữ liệu Parquet
CROWD_COUNTS_PATH = 'data/crowd_counts/*/*.parquet'
ERROR_LOGS_PATH = 'data/error_logs/*/*.parquet'

@lru_cache(maxsize=1) # Cache kết nối để không phải tạo lại liên tục
def get_duckdb_connection():
    """
    Tạo và trả về một kết nối DuckDB.
    Sử dụng cache để tái sử dụng kết nối trong suốt vòng đời ứng dụng.
    """
    return duckdb.connect(database=':memory:', read_only=False)

def query_parquet_as_dataframe(query: str) -> pd.DataFrame:
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
        # Sử dụng .df() để chuyển kết quả trực tiếp sang Pandas DataFrame
        result_df = con.execute(query).df()
        return result_df
    except Exception as e:
        print(f"Lỗi khi thực thi query với DuckDB: {e}")
        # Cân nhắc logging lỗi này
        return pd.DataFrame()

# Bạn có thể thêm các hàm helper khác ở đây, ví dụ:
# def get_total_traffic_for_store(store_name: str):
#     query = f"""
#         SELECT SUM(in_count) as total_in, SUM(out_count) as total_out
#         FROM read_parquet('{CROWD_COUNTS_PATH}', hive_partitioning=1)
#         WHERE store_name = '{store_name}'
#     """
#     return query_parquet_as_dataframe(query)

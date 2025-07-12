import duckdb
import pandas as pd
from functools import lru_cache

# Định nghĩa đường dẫn tới các file Parquet.
# Dấu * giúp DuckDB tự động đọc tất cả các file trong các thư mục con.
CROWD_COUNTS_PATH = 'data/crowd_counts/*/*.parquet'
ERROR_LOGS_PATH = 'data/error_logs/*/*.parquet'

@lru_cache(maxsize=1) # Cache kết nối để không phải tạo lại liên tục
def get_duckdb_connection():
    """
    Tạo và trả về một kết nối DuckDB duy nhất cho ứng dụng.
    Sử dụng lru_cache để đảm bảo kết nối được tái sử dụng, tăng hiệu suất.
    """
    return duckdb.connect(database = ':memory:', read_only = False)

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
        # Thực thi câu lệnh và trả về kết quả dưới dạng Pandas DataFrame
        result_df = con.execute(query).df()
        return result_df
    except Exception as e:
        print(f'Lỗi khi thực thi query với DuckDB: {e}')
        return pd.DataFrame()

# Bạn có thể thêm các hàm helper khác ở đây, ví dụ:
# def get_total_traffic_for_store(store_name: str):
#     query = f"""
#         SELECT SUM(in_count) as total_in, SUM(out_count) as total_out
#         FROM read_parquet('{CROWD_COUNTS_PATH}', hive_partitioning=1)
#         WHERE store_name = '{store_name}'
#     """
#     return query_parquet_as_dataframe(query)

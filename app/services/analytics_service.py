# import pandas as pd
# from ..core import query_parquet_as_dataframe, settings

# def get_all_stores() -> pd.DataFrame:
    
#     query = f"""
#         SELECT DISTINCT store_name
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         ORDER BY store_name;
#     """
#     return query_parquet_as_dataframe(query)

# Bạn có thể thêm các hàm helper khác ở đây, ví dụ:
# from .config import settings
# def get_all_stores() -> pd.DataFrame:
#     query = f"""
#         SELECT DISTINCT store_name
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         ORDER BY store_name;
#     """
#     return query_parquet_as_dataframe(query)

# def get_total_traffic_for_store(store_name: str) -> pd.DataFrame:
#     query = f"""
#         SELECT SUM(in_count) as total_in, SUM(out_count) as total_out
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         WHERE store_name = '{store_name}'
#     """
#     return query_parquet_as_dataframe(query)

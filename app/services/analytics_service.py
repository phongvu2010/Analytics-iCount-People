# import pandas as pd
# from datetime import date, timedelta
# from typing import Optional

# from app.core.data_handler import query_parquet_as_dataframe
# from app.core.config import settings

# def get_time_series_data(
#     period: str, 
#     start_date: date, 
#     end_date: date, 
#     store_name: Optional[str] = None
# ) -> pd.DataFrame:
#     """
#     Lấy dữ liệu chuỗi thời gian cho biểu đồ chính.
#     """
#     where_clauses = [f"record_time BETWEEN '{start_date}' AND '{end_date}'"]
#     if store_name:
#         where_clauses.append(f"store_name = '{store_name}'")
    
#     where_sql = " AND ".join(where_clauses)

#     group_by_col = ""
#     if period == 'day':
#         group_by_col = "EXTRACT(HOUR FROM record_time)"
#         order_by_col = "hour"
#         select_col = f"{group_by_col} AS hour"
#     elif period == 'week':
#         group_by_col = "DAYOFWEEK(record_time)" # 1 (Sun) to 7 (Sat)
#         order_by_col = "day_of_week"
#         select_col = f"{group_by_col} AS day_of_week"
#     elif period == 'month':
#         group_by_col = "EXTRACT(DAY FROM record_time)"
#         order_by_col = "day"
#         select_col = f"{group_by_col} AS day"
#     else: # year
#         group_by_col = "EXTRACT(MONTH FROM record_time)"
#         order_by_col = "month"
#         select_col = f"{group_by_col} AS month"

#     query = f"""
#         SELECT
#             {select_col},
#             SUM(in_count) AS total_in
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         WHERE {where_sql}
#         GROUP BY {group_by_col}
#         ORDER BY {order_by_col};
#     """
#     return query_parquet_as_dataframe(query)

# def get_summary_metrics_data(
#     start_date: date, 
#     end_date: date, 
#     store_name: Optional[str] = None
# ) -> pd.DataFrame:
#     """
#     Lấy dữ liệu cho các thẻ tóm tắt (total, average, peak time, occupancy).
#     """
#     where_clauses = [f"record_time BETWEEN '{start_date}' AND '{end_date}'"]
#     if store_name:
#         where_clauses.append(f"store_name = '{store_name}'")
    
#     where_sql = " AND ".join(where_clauses)

#     query = f"""
#         SELECT
#             SUM(in_count) AS total_in,
#             SUM(in_count - out_count) AS occupancy,
#             EXTRACT(HOUR FROM record_time) AS hour,
#             in_count
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         WHERE {where_sql}
#         GROUP BY hour, in_count;
#     """
#     return query_parquet_as_dataframe(query)

# def get_store_distribution_data(
#     start_date: date, 
#     end_date: date
# ) -> pd.DataFrame:
#     """
#     Lấy dữ liệu phân bổ lượt vào theo từng cửa hàng.
#     """
#     query = f"""
#         SELECT
#             store_name,
#             SUM(in_count) as total_in
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         WHERE record_time BETWEEN '{start_date}' AND '{end_date}'
#         GROUP BY store_name
#         ORDER BY total_in DESC;
#     """
#     return query_parquet_as_dataframe(query)

# def get_error_logs() -> pd.DataFrame:
#     """
#     Lấy tất cả các log lỗi.
#     """
#     query = f"""
#         SELECT id, store_name, log_time, error_code, error_message
#         FROM read_parquet('{settings.ERROR_LOGS_PATH}', hive_partitioning=true)
#         ORDER BY log_time DESC;
#     """
#     return query_parquet_as_dataframe(query)

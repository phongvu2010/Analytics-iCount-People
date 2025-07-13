# import pandas as pd
# from datetime import date, timedelta
# from typing import Optional

# from ..core import query_parquet_as_dataframe, settings

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
#         SELECT {select_col}, SUM(in_count) AS total_in
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










# # def get_traffic_data(period: str, store_name: str = None) -> pd.DataFrame:
# #     """
# #     Lấy dữ liệu thống kê lưu lượng người ra vào theo khoảng thời gian.

# #     Args:
# #         period (str): Khoảng thời gian thống kê ('day', 'week', 'month', 'year').
# #         store_name (str, optional): Tên cửa hàng để lọc. Mặc định là tất cả.

# #     Returns:
# #         pd.DataFrame: DataFrame chứa dữ liệu đã được tổng hợp.
# #     """
# #     # Chọn định dạng thời gian cho việc nhóm dữ liệu (GROUP BY)
# #     time_format_mapping = {
# #         'day': '%Y-%m-%d',      # Theo ngày: 2023-10-27
# #         'week': '%Y-W%W',       # Theo tuần: 2023-W43
# #         'month': '%Y-%m',       # Theo tháng: 2023-10
# #         'year': '%Y'            # Theo năm: 2023
# #     }

# #     time_format = time_format_mapping.get(period.lower(), '%Y-%m-%d')

# #     # Xây dựng mệnh đề WHERE để lọc theo cửa hàng nếu có
# #     where_clause = ""
# #     if store_name and store_name.lower() != 'all':
# #         where_clause = f"WHERE store_name = '{store_name}'"

# #     # Xây dựng câu lệnh SQL
# #     # `hive_partitioning=true` giúp DuckDB hiểu cấu trúc thư mục year=...
# #     query = f"""
# #         SELECT
# #             strftime(record_time, '{time_format}') AS period,
# #             SUM(in_count) AS total_in,
# #             SUM(out_count) AS total_out
# #         FROM read_parquet('{CROWD_COUNTS_PATH}', hive_partitioning=true)
# #         {where_clause}
# #         GROUP BY period
# #         ORDER BY period ASC;
# #     """
# #     return query_parquet_as_dataframe(query)

# # def get_distinct_stores() -> list:
# #     """
# #     Lấy danh sách các cửa hàng duy nhất từ dữ liệu.
# #     """
# #     query = f"""
# #         SELECT DISTINCT store_name
# #         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
# #         ORDER BY store_name;
# #     """
# #     df = query_parquet_as_dataframe(query)

# #     if not df.empty:
# #         return df['store_name'].tolist()
# #     return []

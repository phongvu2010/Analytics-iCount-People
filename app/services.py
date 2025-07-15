# import pandas as pd
# from datetime import datetime, timedelta

from .core.config import settings
from .core.data_handler import query_parquet_as_dataframe


def get_dashboard_data(period: str, store_id: str, start_date: str, end_date: str):
    """
    Xây dựng câu lệnh SQL, truy vấn và xử lý dữ liệu cho dashboard.
    """
    # 1. Xây dựng câu lệnh SQL dựa trên bộ lọc
    # Ví dụ: chọn cột, nhóm theo thời gian, lọc theo ngày và cửa hàng
    # DuckDB có các hàm thời gian rất mạnh (ví dụ: YEAR(), MONTH(), DAYOFWEEK())
    query = f"""
        SELECT
            CAST(record_time AS TIMESTAMP) as record_time,
            in_count,
            out_count,
            store_name
        FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
        WHERE record_time BETWEEN '{start_date}' AND '{end_date}'
    """
    if store_id != 'all':
        query += f" AND store_name = '{store_id}'"

    df = query_parquet_as_dataframe(query)

    if df.empty:
        # Xử lý trường hợp không có dữ liệu
        return None

    # 2. Xử lý DataFrame với Pandas để tính toán các chỉ số
    # Ví dụ:
    # - Tổng lượt vào: df['in_count'].sum()
    # - Lượt vào trung bình: df['in_count'].mean()
    # - Giờ cao điểm: df.groupby(df['record_time'].dt.hour)['in_count'].sum().idxmax()
    # - ... và các chỉ số khác bạn cần

    # 3. Định dạng dữ liệu trả về theo các schema đã định nghĩa
    # ... (logic xử lý và định dạng)

    # Dữ liệu trả về cuối cùng sẽ có dạng của schema DashboardData
    # Ví dụ (đây là dữ liệu giả, bạn cần thay bằng logic thật):
    mock_data = {
        "metrics": {
            "total_in": 12345,
            "average_in": 514.3,
            "peak_time": "19:00",
            "occupancy": 150,
            "busiest_store": "Cửa chính A1",
            "growth_percentage": 15.2
        },
        "line_chart_data": [{"label": "10:00", "value": 100}, {"label": "11:00", "value": 150}],
        "store_comparison_data": {"labels": ["Cửa A1", "Cửa A2"], "data": [7000, 5345]},
        "table_data": [{"label": "10:00", "value": 100}, {"label": "11:00", "value": 150}]
    }
    return mock_data

def get_error_logs():
    """
    Lấy các log lỗi gần nhất.
    """
    query = f"""
        SELECT store_name, log_time, error_message
        FROM read_parquet('{settings.ERROR_LOGS_PATH}')
        ORDER BY log_time DESC
        LIMIT 10;
    """
    df = query_parquet_as_dataframe(query)
    return df.to_dict(orient='records')

def get_all_stores():
    """
    Lấy danh sách các cửa hàng duy nhất.
    """
    query = f"""
        SELECT DISTINCT store_name
        FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
        WHERE store_name IS NOT NULL;
    """
    df = query_parquet_as_dataframe(query)
    # Giả sử bạn muốn trả về dạng [{id: 1, name: 'Cửa A'}, ...]
    df['id'] = range(1, len(df) + 1)
    return df[['id', 'store_name']].rename(columns={'store_name': 'name'}).to_dict(orient='records')









# from datetime import date
# from typing import Optional

# from .core import query_parquet_as_dataframe, settings

# def get_all_stores() -> pd.DataFrame:
#     """
#     Lấy danh sách tất cả các cửa hàng (store_name) duy nhất từ dữ liệu.
#     """
#     query = f"""
#         SELECT DISTINCT store_name
#         FROM read_parquet('{settings.CROWD_COUNTS_PATH}', hive_partitioning=true)
#         WHERE store_name IS NOT NULL
#         ORDER BY store_name;
#     """
#     return query_parquet_as_dataframe(query)

# def get_error_logs(limit: int=100) -> pd.DataFrame:
#     """
#     Lấy tất cả các log lỗi.
#     """
#     query = f"""
#         SELECT id, store_name, log_time, error_code, error_message
#         FROM read_parquet('{settings.ERROR_LOGS_PATH}', hive_partitioning=true)
#         ORDER BY log_time DESC
#         LIMIT {limit};
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

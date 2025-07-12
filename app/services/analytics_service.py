import pandas as pd
from app.core.data_handler import query_parquet_as_dataframe, CROWD_COUNTS_PATH

def get_traffic_data(period: str, store_name: str = None) -> pd.DataFrame:
    """
    Lấy dữ liệu thống kê lưu lượng người ra vào theo khoảng thời gian.

    Args:
        period (str): Khoảng thời gian thống kê ('day', 'week', 'month', 'year').
        store_name (str, optional): Tên cửa hàng để lọc. Mặc định là tất cả.

    Returns:
        pd.DataFrame: DataFrame chứa dữ liệu đã được tổng hợp.
    """
    # Chọn định dạng thời gian cho việc nhóm dữ liệu (GROUP BY)
    time_format_mapping = {
        'day': '%Y-%m-%d',      # Theo ngày: 2023-10-27
        'week': '%Y-W%W',       # Theo tuần: 2023-W43
        'month': '%Y-%m',       # Theo tháng: 2023-10
        'year': '%Y'            # Theo năm: 2023
    }

    time_format = time_format_mapping.get(period.lower(), '%Y-%m-%d')

    # Xây dựng mệnh đề WHERE để lọc theo cửa hàng nếu có
    where_clause = ""
    if store_name and store_name.lower() != 'all':
        where_clause = f"WHERE store_name = '{store_name}'"

    # Xây dựng câu lệnh SQL
    # `hive_partitioning=1` giúp DuckDB hiểu cấu trúc thư mục year=...
    query = f"""
        SELECT
            strftime(record_time, '{time_format}') AS period,
            SUM(in_count) AS total_in,
            SUM(out_count) AS total_out
        FROM read_parquet('{CROWD_COUNTS_PATH}', hive_partitioning=1)
        {where_clause}
        GROUP BY period
        ORDER BY period ASC;
    """

    return query_parquet_as_dataframe(query)

def get_distinct_stores() -> list:
    """
    Lấy danh sách các cửa hàng duy nhất từ dữ liệu.
    """
    query = f"""
        SELECT DISTINCT store_name
        FROM read_parquet('{CROWD_COUNTS_PATH}', hive_partitioning=1)
        ORDER BY store_name;
    """
    df = query_parquet_as_dataframe(query)
    if not df.empty:
        return df['store_name'].tolist()
    return []

from datetime import date, timedelta
from typing import List, Dict, Any

from .core.data_handler import query_parquet_as_dataframe
from .core.config import settings

class DashboardService:
    """
    Lớp chứa tất cả logic nghiệp vụ để lấy dữ liệu cho dashboard.
    """
    def __init__(self, start_date: date, end_date: date, store: str = 'all'):
        self.start_date = start_date
        self.end_date = end_date

        # DuckDB xử lý ngày tháng hiệu quả với kiểu DATE
        self.start_date_str = self.start_date.strftime('%Y-%m-%d')
        self.end_date_str = (self.end_date + timedelta(days=1)).strftime('%Y-%m-%d') # Bao gồm cả ngày kết thúc
        self.store_filter = f"AND store_name = '{store}'" if store != 'all' else ""

        # Tạo CTE (Common Table Expression) để tái sử dụng bộ dữ liệu đã lọc
        self.base_cte = f"""
        WITH filtered_data AS (
            SELECT
                CAST(record_time AS TIMESTAMP) as record_time,
                store_name,
                CASE
                    WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)
                    ELSE in_count
                END as in_count,
                CASE
                    WHEN out_count > {settings.OUTLIER_THRESHOLD} THEN CAST(ROUND(out_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)
                    ELSE out_count
                END as out_count
            FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
            WHERE record_time >= '{self.start_date_str}' AND record_time < '{self.end_date_str}'
            {self.store_filter}
        )
        """

    def get_metrics(self) -> Dict[str, Any]:
        """Lấy các chỉ số chính (total, average, peak time...)."""
        query = f"""
        {self.base_cte}
        SELECT
            SUM(in_count) as total_in,
            AVG(in_count) as average_in,
            strftime(arg_max(record_time, in_count), '%H:%M') as peak_time,
            (SUM(in_count) - SUM(out_count)) as current_occupancy,
            -- arg_max(store_name, total_store_in) as busiest_store
            -- Lấy cửa hàng bận rộn nhất từ subquery đã được tổng hợp
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        FROM filtered_data
        -- CROSS JOIN (
        --     SELECT store_name, SUM(in_count) as total_store_in
        --     FROM filtered_data
        --     GROUP BY store_name
        -- )
        """
        df = query_parquet_as_dataframe(query)
        if df.empty or df['total_in'].iloc[0] is None:
            return { "total_in": 0, "average_in": 0, "peak_time": "--:--", "current_occupancy": 0, "busiest_store": "N/A", "growth": 0.0 }

        # Logic tính toán tăng trưởng (ví dụ: so với kỳ trước)
        # Tạm thời để giá trị giả định
        growth = 15.5 

        data = df.iloc[0].to_dict()
        data['average_in'] = round(data.get('average_in', 0), 1)
        data['growth'] = growth
        return data

    def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu cho biểu đồ đường (xu hướng theo thời gian)."""
        time_unit = 'hour' # Mặc định, có thể thay đổi tùy theo khoảng thời gian
        if self.end_date - self.start_date > timedelta(days=30):
            time_unit = 'day'

        query = f"""
        {self.base_cte}
        SELECT
            date_trunc('{time_unit}', record_time) as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY x
        """
        df = query_parquet_as_dataframe(query)
        return df.to_dict(orient='records')

    def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu cho biểu đồ donut (tỷ trọng theo cửa)."""
        query = f"""
        {self.base_cte}
        SELECT
            store_name as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        df = query_parquet_as_dataframe(query)
        return df.to_dict(orient='records')

    def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
        """Lấy dữ liệu chi tiết cho bảng, có phân trang."""
        offset = (page - 1) * page_size

        count_query = f"{self.base_cte} SELECT COUNT(*) as total FROM filtered_data"
        total_records = query_parquet_as_dataframe(count_query)['total'].iloc[0]

        data_query = f"""
        {self.base_cte}
        SELECT record_time, store_name, in_count, out_count
        FROM filtered_data
        ORDER BY record_time DESC
        LIMIT {page_size} OFFSET {offset}
        """
        df = query_parquet_as_dataframe(data_query)

        return {
            "total_records": int(total_records),
            "page": page,
            "page_size": page_size,
            "data": df.to_dict(orient='records')
        }

    @staticmethod
    def get_all_stores() -> List[str]:
        """Lấy danh sách tất cả các cửa hàng."""
        query = f"SELECT DISTINCT store_name FROM read_parquet('{settings.CROWD_COUNTS_PATH}') ORDER BY store_name"
        df = query_parquet_as_dataframe(query)
        return df['store_name'].tolist()

    @staticmethod
    def get_error_logs(limit: int = 10) -> List[Dict[str, Any]]:
        """Lấy các log lỗi gần nhất."""
        query = f"""
        SELECT id, store_name, log_time, error_code, error_message
        FROM read_parquet('{settings.ERROR_LOGS_PATH}')
        ORDER BY log_time DESC
        LIMIT {limit}
        """
        df = query_parquet_as_dataframe(query)
        return df.to_dict(orient='records')

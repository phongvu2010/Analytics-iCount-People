import asyncio
import pandas as pd

from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple

from .core.caching import async_cache
from .core.config import settings
from .core.data_handler import query_parquet_as_dataframe

class DashboardService:
    """
    Lớp chứa tất cả logic nghiệp vụ để lấy dữ liệu cho dashboard.
    Đã được refactor để giảm lặp code và sử dụng async/await cho hiệu năng tốt hơn.
    """
    def __init__(self, period: str, start_date: date, end_date: date, store: str = 'all'):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(self, start_date: date, end_date: date) -> Tuple[str, str]:
        """
        Helper để tạo chuỗi thời gian query dựa trên "ngày làm việc".
        """
        start_dt = datetime.combine(start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)
        return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')

    def _build_base_query(self, start_date_str: str, end_date_str: str) -> Tuple[str, list]:
        """
        [REFACTOR] Xây dựng câu query CTE gốc và các tham số.
        Hàm này được tái sử dụng trong tất cả các phương thức lấy dữ liệu.
        """
        params = [start_date_str, end_date_str]

        store_filter_clause = ''
        if self.store != 'all':
            store_filter_clause = 'AND store_name = ?'
            params.append(self.store)

        if settings.OUTLIER_SCALE_RATIO != 0:
            then_logic_in = f'CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
            then_logic_out = f'CAST(ROUND(out_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
        else:
            then_logic_in, then_logic_out = '1', '1'

        base_cte = f"""
        WITH source_data AS (
            SELECT
                CAST(record_time AS TIMESTAMP) as record_time,
                store_name,
                CASE
                    WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in} ELSE in_count
                    END as in_count,
                CASE
                    WHEN out_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out} ELSE out_count
                    END as out_count
            FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
            WHERE record_time >= ? AND record_time < ?
            {store_filter_clause}
        ),
        filtered_data AS (
            SELECT *, (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
            FROM source_data
        )
        """
        return base_cte, params

    async def _get_previous_period_total_in(self) -> int:
        """
        [REFACTOR] Tính toán tổng lượt vào của kỳ trước, được viết lại gọn gàng hơn.
        """
        time_delta = self.end_date - self.start_date
        
        # [UPDATE] Sử dụng dictionary để quản lý logic tính toán cho từng kỳ
        period_logic = {
            'day': {
                'start': self.start_date - (time_delta + timedelta(days=1)),
                'end': self.end_date - (time_delta + timedelta(days=1))
            },
            'week': {
                'start': self.start_date - timedelta(weeks=1),
                'end': self.end_date - timedelta(weeks=1)
            },
            'month': {
                # Logic cho tháng trước: từ ngày đầu của tháng trước đến cuối tháng trước
                'start': self.start_date - relativedelta(months=1),
                'end': self.start_date - timedelta(days=1)
            },
            'year': {
                'start': self.start_date - relativedelta(years=1),
                'end': self.end_date - relativedelta(years=1)
            }
        }

        dates = period_logic.get(self.period)

        if not dates:
            return 0

        start_str, end_str = self._get_date_range_params(dates['start'], dates['end'])
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f'{base_cte} SELECT SUM(in_count) as total FROM filtered_data'
        
        df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)

        if df.empty or df['total'].iloc[0] is None:
            return 0
        return int(df['total'].iloc[0])

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """
        [ASYNC] Lấy các chỉ số chính (total, average, peak time...).
        ĐÃ SỬA LỖI NaN KHI KHÔNG CÓ DỮ LƯỢU.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)
        
        peak_time_format = {'day': '%H:%M', 'week': '%d/%m', 'month': '%d/%m', 'year': 'Tháng %m'}.get(self.period, '%d/%m')
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        , period_summary AS (
            SELECT SUM(in_count) as total_in_per_period FROM filtered_data
            GROUP BY date_trunc('{time_unit}', adjusted_time)
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (SELECT strftime(arg_max(record_time, in_count), '{peak_time_format}') FROM filtered_data) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """

        df, total_in_previous = await asyncio.gather(
            asyncio.to_thread(query_parquet_as_dataframe, query, params=params),
            self._get_previous_period_total_in()
        )

        # 1. Trả về giá trị mặc định ngay nếu DataFrame rỗng
        if df.empty or pd.isna(df['total_in'].iloc[0]):
            return {'total_in': 0, 'average_in': 0, 'peak_time': '--:--', 'current_occupancy': 0, 'busiest_store': 'N/A', 'growth': 0.0}

        data = df.iloc[0].to_dict()
        total_in_current = data.get('total_in', 0) or 0

        growth = 0.0
        if total_in_previous > 0:
            growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
        elif total_in_current > 0:
            growth = 100.0

        # 2. Xử lý giá trị average_in một cách an toàn
        avg_val = data.get('average_in')
        # pd.isna() kiểm tra được cả None và NaN
        data['average_in'] = 0 if pd.isna(avg_val) else int(round(avg_val))

        data['growth'] = growth
        if data.get('busiest_store'):
            data['busiest_store'] = data['busiest_store'].split(' (')[0]

        return data

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """
        [ASYNC] Lấy dữ liệu cho biểu đồ đường.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        SELECT
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x ORDER BY x
        """

        df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)

        if time_unit == 'month': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """
        [ASYNC] Lấy dữ liệu cho biểu đồ donut (tỷ trọng theo cửa).
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f"""
        {base_cte}
        SELECT store_name as x, SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)
        return df.to_dict(orient='records')

    @async_cache
    async def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
        """
        [ASYNC] Lấy dữ liệu chi tiết cho bảng, có phân trang.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')
        date_format = {'hour': '%Y-%m-%d %H:00', 'day': '%Y-%m-%d', 'month': '%Y-%m'}.get(time_unit, '%Y-%m-%d')

        aggregation_query = f"""
        {base_cte}
        , period_summary AS (
            SELECT date_trunc('{time_unit}', adjusted_time) as period_start, SUM(in_count) as total_in
            FROM filtered_data GROUP BY period_start
        ),
        period_summary_with_lag AS (
            SELECT *, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
            FROM period_summary
        )
        SELECT
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE WHEN previous_period_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1) END as pct_change
        FROM period_summary_with_lag
        """

        final_query_cte = f"WITH query_result AS ({aggregation_query})"

        data_query = f"{final_query_cte} SELECT * FROM query_result ORDER BY period DESC LIMIT ? OFFSET ?"
        summary_query = f"{final_query_cte} SELECT SUM(total_in) as total_sum, AVG(total_in) as average_in FROM query_result"

        paginated_params = params + [page_size, (page - 1) * page_size]

        df, summary_df = await asyncio.gather(
            asyncio.to_thread(query_parquet_as_dataframe, data_query, params=paginated_params),
            asyncio.to_thread(query_parquet_as_dataframe, summary_query, params=params)
        )

        summary_data = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total_sum': 0, 'average_in': 0}

        return {
            'total_records': len(df),
            'page': page,
            'page_size': page_size,
            'data': df.to_dict(orient='records'),
            'summary': summary_data
        }

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """
        Lấy ra timestamp của bản ghi gần nhất.
        """
        query = f"""
        SELECT MAX(record_time) as latest_time
        FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
        """
        df = query_parquet_as_dataframe(query)
        if not df.empty and pd.notna(df['latest_time'].iloc[0]):
            return df['latest_time'].iloc[0]
        return None

    @staticmethod
    def get_all_stores() -> List[str]:
        """
        Lấy danh sách tất cả các cửa hàng.
        """
        query = f"""
        SELECT DISTINCT store_name
        FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
        ORDER BY store_name
        """
        df = query_parquet_as_dataframe(query)
        return df['store_name'].tolist()

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """
        Lấy các log lỗi gần nhất.
        """
        query = f"""
        SELECT id, store_name, log_time, error_code, error_message
        FROM read_parquet('{settings.ERROR_LOGS_PATH}')
        ORDER BY log_time DESC
        LIMIT ?
        """
        df = query_parquet_as_dataframe(query, params=[limit])
        return df.to_dict(orient='records')

"""
Module chứa lớp Service chịu trách nhiệm xử lý logic nghiệp vụ.

Lớp `DashboardService` đóng gói tất cả các phương thức cần thiết để truy vấn,
tính toán và định dạng dữ liệu cho dashboard từ kho dữ liệu DuckDB.
"""
import asyncio
import logging
import pandas as pd

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, List, Optional, Tuple

from .core.caching import async_cache
from .core.config import settings
from .dependencies import query_db_to_df

logger = logging.getLogger(__name__)


class DashboardService:
    """
    Lớp chứa logic nghiệp vụ để xử lý và truy vấn dữ liệu cho dashboard.

    Mỗi instance của lớp này tương ứng với một bộ lọc (thời gian, cửa hàng)
    cụ thể từ người dùng, đóng vai trò là context cho tất cả các truy vấn
    dữ liệu liên quan.
    """
    def __init__(
        self, period: str, start_date: date, end_date: date, store: str = 'all'
    ):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(
        self, start_date: date, end_date: date
    ) -> Tuple[str, str]:
        """
        Tạo chuỗi thời gian cho query dựa trên định nghĩa "ngày làm việc".

        Giờ làm việc có thể kéo dài qua nửa đêm (ví dụ: 9h sáng đến 2h sáng
        hôm sau). Hàm này điều chỉnh ngày bắt đầu và kết thúc để bao trọn
        khung giờ này khi truy vấn.
        """
        start_dt = datetime.combine(
            start_date, datetime.min.time()
        ) + timedelta(hours=settings.WORKING_HOUR_START)

        end_dt = datetime.combine(
            end_date, datetime.min.time()
        ) + timedelta(days=1, hours=settings.WORKING_HOUR_END)

        return (
            start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_dt.strftime('%Y-%m-%d %H:%M:%S')
        )


    # ================== THAY ĐỔI: XÓA BỎ HOÀN TOÀN HÀM `_build_base_query` ==================
    # Toàn bộ logic của hàm này đã được chuyển vào VIEW `v_traffic_normalized`
    # trong database, được quản lý bởi lệnh `cli.py init-db`.
    # ======================================================================================
    
    # def _build_base_query(
    #     self, start_date_str: str, end_date_str: str
    # ) -> Tuple[str, list]:
    #     """
    #     Xây dựng câu truy vấn CTE (Common Table Expression) cơ sở.

    #     Hàm này tạo ra một CTE chuẩn hóa dữ liệu nguồn, bao gồm:
    #     - Lọc dữ liệu theo khoảng thời gian và cửa hàng.
    #     - Xử lý các giá trị ngoại lệ (outliers) theo cấu hình.
    #     - Điều chỉnh timestamp để phân tích theo "ngày làm việc".

    #     CTE này được tái sử dụng trong nhiều phương thức khác để tránh lặp code.
    #     """
    #     params = [start_date_str, end_date_str]

    #     store_filter_clause = ''
    #     if self.store != 'all':
    #         store_filter_clause = 'AND store_name = ?'
    #         params.append(self.store)

    #     # Xử lý outlier: thay thế các giá trị quá lớn bằng một tỷ lệ nhỏ
    #     # hoặc một giá trị cố định.
    #     scale = settings.OUTLIER_SCALE_RATIO
    #     then_logic_in = f'CAST(ROUND(a.visitors_in * {scale}, 0) AS INTEGER)' if scale > 0 else '1'
    #     then_logic_out = f'CAST(ROUND(a.visitors_out * {scale}, 0) AS INTEGER)' if scale > 0 else '1'

    #     base_cte = f"""
    #     WITH source_data AS (
    #         SELECT
    #             CAST(a.recorded_at AS TIMESTAMP) as record_time,
    #             b.store_name,
    #             CASE
    #                 WHEN a.visitors_in > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in}
    #                 ELSE a.visitors_in
    #             END as in_count,
    #             CASE
    #                 WHEN a.visitors_out > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out}
    #                 ELSE a.visitors_out
    #             END as out_count
    #         FROM fact_traffic AS a
    #         LEFT JOIN dim_stores AS b ON a.store_id = b.store_id
    #         WHERE
    #             a.recorded_at >= ? AND a.recorded_at < ?
    #             {store_filter_clause}
    #     ),
    #     filtered_data AS (
    #         -- Dịch chuyển thời gian để ngày làm việc bắt đầu từ 00:00
    #         SELECT *, (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
    #         FROM source_data
    #     )
    #     """
    #     return base_cte, params

    def _get_base_filters(self) -> Tuple[str, list]:
        """
        Helper mới để tạo mệnh đề WHERE và tham số cho các truy vấn.
        Hàm này thay thế việc phải xây dựng lại logic filter ở nhiều nơi.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        params = [start_str, end_str]
        
        filter_clauses = "WHERE record_time >= ? AND record_time < ?"
        
        if self.store != 'all':
            filter_clauses += " AND store_name = ?"
            params.append(self.store)
            
        return filter_clauses, params

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Lấy các chỉ số chính (KPIs) cho dashboard.

        Bao gồm tổng lượt vào, trung bình, giờ cao điểm, lượng khách hiện tại,
        cửa hàng đông nhất và tỷ lệ tăng trưởng so với kỳ trước.
        """
        filter_clauses, params = self._get_base_filters()
        # start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        # base_cte, params = self._build_base_query(start_str, end_str)

        # Định dạng và đơn vị thời gian cho truy vấn
        time_unit_map = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}
        time_unit = time_unit_map.get(self.period, 'day')
        peak_time_format_map = {'day': '%H:%M', 'week': '%d/%m', 'month': '%d/%m', 'year': 'Tháng %m'}
        peak_time_format = peak_time_format_map.get(self.period, '%d/%m')

        # Câu truy vấn giờ đây sạch hơn, chỉ tập trung vào logic tổng hợp
        query = f"""
        WITH filtered_data AS (
            SELECT * FROM v_traffic_normalized
            {filter_clauses}
        ), 
        period_summary AS (
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period,
                SUM(in_count) as total_in_per_period
            FROM filtered_data
            GROUP BY period
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (
                SELECT strftime(period + INTERVAL '{settings.WORKING_HOUR_START} hours', '{peak_time_format}')
                FROM period_summary ORDER BY total_in_per_period DESC LIMIT 1
            ) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """
        # query = f"""
        # {base_cte}
        # , period_summary AS (
        #     SELECT
        #         date_trunc('{time_unit}', adjusted_time) as period,
        #         SUM(in_count) as total_in_per_period
        #     FROM filtered_data
        #     GROUP BY period
        # )
        # SELECT
        #     (SELECT SUM(in_count) FROM filtered_data) as total_in,
        #     (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
        #     (
        #         SELECT strftime(period + INTERVAL '{settings.WORKING_HOUR_START} hours', '{peak_time_format}')
        #         FROM period_summary ORDER BY total_in_per_period DESC LIMIT 1
        #     ) as peak_time,
        #     (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
        #     (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        # """

        df, prev_total = await asyncio.gather(
            asyncio.to_thread(query_db_to_df, query, params=params),
            self._get_previous_period_total_in()
        )

        if df.empty or pd.isna(df['total_in'].iloc[0]):
            return {
                'total_in': 0,
                'average_in': 0,
                'peak_time': '--:--',
                'current_occupancy': 0,
                'busiest_store': 'N/A',
                'growth': 0.0
            }

        data = df.iloc[0].to_dict()
        current_total = data.get('total_in', 0) or 0
        growth = 0.0
        if prev_total > 0:
            growth = round(((current_total - prev_total) / prev_total) * 100, 1)
        elif current_total > 0:
            growth = 100.0

        avg_val = data.get('average_in')
        data['average_in'] = 0 if pd.isna(avg_val) else int(round(avg_val))
        data['growth'] = growth
        if data.get('busiest_store'):
            data['busiest_store'] = data['busiest_store'].split(' (')[0]

        return data

    async def _get_previous_period_total_in(self) -> int:
        """
        Tính tổng lượt khách của kỳ liền trước để so sánh tăng trưởng.
        """
        dates = {
            'day': {
                'start': self.start_date - timedelta(days=1),
                'end': self.end_date - timedelta(days=1)
            },
            'week': {
                'start': self.start_date - timedelta(weeks=1),
                'end': self.end_date - timedelta(weeks=1)
            },
            'month': {
                'start': self.start_date - relativedelta(months=1),
                'end': self.start_date - timedelta(days=1)
            },
            'year': {
                'start': self.start_date - relativedelta(years=1),
                'end': self.end_date - relativedelta(years=1)
            }
        }.get(self.period)

        if not dates: return 0

        # Tái sử dụng logic filter tương tự
        start_str, end_str = self._get_date_range_params(dates['start'], dates['end'])
        params = [start_str, end_str]
        # base_cte, params = self._build_base_query(start_str, end_str)
        filter_clauses = "WHERE record_time >= ? AND record_time < ?"
        if self.store != 'all':
            filter_clauses += " AND store_name = ?"
            params.append(self.store)
        
        query = f'SELECT SUM(in_count) as total FROM v_traffic_normalized {filter_clauses}'
        # query = f'{base_cte} SELECT SUM(in_count) as total FROM filtered_data'
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        return 0 if df.empty or pd.isna(df['total'].iloc[0]) else int(df['total'].iloc[0])

    @staticmethod
    def get_all_stores() -> List[str]:
        """
        Lấy danh sách duy nhất tất cả các cửa hàng.
        """
        df = query_db_to_df("SELECT DISTINCT store_name FROM dim_stores ORDER BY store_name")
        return [] if df.empty else df['store_name'].tolist()

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu chuỗi thời gian cho biểu đồ xu hướng.
        """
        filter_clauses, params = self._get_base_filters()
        # start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        # base_cte, params = self._build_base_query(start_str, end_str)
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        SELECT
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM v_traffic_normalized
        {filter_clauses}
        GROUP BY x ORDER BY x
        """
        # query = f"""
        # {base_cte}
        # SELECT
        #     (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
        #     SUM(in_count) as y
        # FROM filtered_data
        # GROUP BY x ORDER BY x
        # """
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        if time_unit == 'month':
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day':
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: # hour
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu phân bổ lượt khách theo từng cửa hàng.
        """
        filter_clauses, params = self._get_base_filters()
        # start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        # base_cte, params = self._build_base_query(start_str, end_str)
        query = f"""
            SELECT store_name as x, SUM(in_count) as y 
            FROM v_traffic_normalized 
            {filter_clauses} 
            GROUP BY x ORDER BY y DESC
        """
        # query = f"{base_cte} SELECT store_name as x, SUM(in_count) as y FROM filtered_data GROUP BY x ORDER BY y DESC"
        df = await asyncio.to_thread(query_db_to_df, query, params=params)
        return df.to_dict(orient='records')

    @async_cache
    async def get_table_details(self) -> Dict[str, Any]:
        """
        Lấy dữ liệu chi tiết cho bảng, giới hạn 31 dòng gần nhất.
        """
        filter_clauses, params = self._get_base_filters()
        # start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        # base_cte, params = self._build_base_query(start_str, end_str)
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')
        date_format = {'hour': '%Y-%m-%d %H:00', 'day': '%Y-%m-%d', 'month': '%Y-%m'}.get(time_unit, '%Y-%m-%d')

        query = f"""
        WITH filtered_data AS (
            SELECT * FROM v_traffic_normalized {filter_clauses}
        ),
        aggregated AS (
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period_start,
                SUM(in_count) as total_in
            FROM filtered_data GROUP BY period_start
        ),
        with_lag AS (
            SELECT *, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_in
            FROM aggregated
        )
        SELECT
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE WHEN previous_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_in) * 100.0) / previous_in, 1) END as pct_change
        FROM with_lag
        ORDER BY period_start DESC
        LIMIT 31
        """
        # query = f"""
        # WITH aggregated AS (
        #     {base_cte}
        #     SELECT
        #         date_trunc('{time_unit}', adjusted_time) as period_start,
        #         SUM(in_count) as total_in
        #     FROM filtered_data GROUP BY period_start
        # ),
        # with_lag AS (
        #     SELECT
        #         *,
        #         LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_in
        #     FROM aggregated
        # )
        # SELECT
        #     strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
        #     total_in,
        #     CASE WHEN previous_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_in) * 100.0) / previous_in, 1) END as pct_change
        # FROM with_lag
        # ORDER BY period_start DESC
        # LIMIT 31
        # """

        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        if df.empty:
            return {'data': [], 'summary': {'total_sum': 0, 'average_in': 0}}

        total_sum = df['total_in'].sum()
        df['proportion_pct'] = (df['total_in'] / total_sum * 100) if total_sum > 0 else 0.0
        df['proportion_change'] = df['proportion_pct'].diff(periods=-1).fillna(0)

        summary = {'total_sum': total_sum, 'average_in': df['total_in'].mean()}

        return {'data': df.to_dict(orient='records'), 'summary': summary}

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """
        Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu.
        """
        df = query_db_to_df("SELECT MAX(recorded_at) as latest_time FROM fact_traffic")
        return df['latest_time'].iloc[0] if not df.empty and pd.notna(df['latest_time'].iloc[0]) else None

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """
        Lấy các log lỗi gần nhất.
        """
        query = """
        SELECT a.log_id as id, b.store_name, a.logged_at as log_time, a.error_code, a.error_message
        FROM fact_errors AS a
        LEFT JOIN dim_stores AS b ON a.store_id = b.store_id
        ORDER BY a.logged_at DESC
        LIMIT ?
        """
        df = query_db_to_df(query, params=[limit])
        return df.to_dict(orient='records')

import asyncio
import logging
import pandas as pd

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from duckdb import DuckDBPyConnection
from typing import Any, Dict, List, Optional, Tuple

from .core.caching import async_cache
from .core.config import settings
from .core.dependencies import query_db_to_df

logger = logging.getLogger(__name__)

# LƯU Ý: service bây giờ sẽ cần một kết nối DB để hoạt động.
# Tuy nhiên, để giữ sự đơn giản như thiết kế ban đầu của bạn,
# chúng ta sẽ sử dụng hàm tiện ích `query_db_to_df` thay vì inject
# connection vào constructor. Cách này giữ cho logic gọi hàm không đổi.
# Nếu dự án phức tạp hơn, việc inject connection vào __init__ sẽ tốt hơn.
class DashboardService:
    """
    Lớp chứa logic nghiệp vụ để xử lý và truy vấn dữ liệu cho dashboard.

    Mỗi instance của lớp này tương ứng với một bộ lọc (thời gian, cửa hàng)
    cụ thể từ người dùng, đóng vai trò là context cho các truy vấn dữ liệu.
    """
    def __init__(self, period: str, start_date: date, end_date: date, store: str = 'all'):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(self, start_date: date, end_date: date) -> Tuple[str, str]:
        """
        Tạo chuỗi thời gian query dựa trên `ngày làm việc` đã định nghĩa.

        Giờ làm việc có thể kéo dài qua nửa đêm (ví dụ: 9h sáng đến 2h sáng hôm sau).
        Hàm này điều chỉnh ngày bắt đầu và kết thúc để bao trọn khung giờ này.
        """
        start_dt = datetime.combine(start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)
        return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')

    def _build_base_query(self, start_date_str: str, end_date_str: str) -> Tuple[str, list]:
        """
        Xây dựng câu truy vấn CTE (Common Table Expression) cơ sở và tham số.

        Hàm này tạo ra một CTE chuẩn hóa dữ liệu nguồn, bao gồm:
        - Lọc dữ liệu theo khoảng thời gian và cửa hàng.
        - Xử lý các giá trị ngoại lệ (outliers) theo cấu hình.
        - Điều chỉnh timestamp để phân tích theo "ngày làm việc".
        CTE này được tái sử dụng trong nhiều phương thức khác để tránh lặp code.
        """
        params = [start_date_str, end_date_str]

        store_filter_clause = ''
        if self.store != 'all':
            store_filter_clause = 'AND store_name = ?'
            params.append(self.store)

        # Xử lý outlier: thay thế các giá trị quá lớn bằng một tỷ lệ nhỏ hoặc giá trị cố định.
        if settings.OUTLIER_SCALE_RATIO > 0:
            then_logic_in = f'CAST(ROUND(a.visitors_in * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
            then_logic_out = f'CAST(ROUND(a.visitors_out * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
        else:
            # Nếu không muốn scale, có thể thay bằng một giá trị mặc định, ví dụ là 1.
            then_logic_in, then_logic_out = '1', '1'

        base_cte = f"""
        WITH source_data AS (
            SELECT
                CAST(a.recorded_at AS TIMESTAMP) as record_time,
                b.store_name,
                CASE WHEN a.visitors_in > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in} ELSE a.visitors_in END as in_count,
                CASE WHEN a.visitors_out > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out} ELSE a.visitors_out END as out_count
            FROM fact_traffic AS a
            LEFT JOIN dim_stores AS b ON a.store_id = b.store_id
            WHERE a.recorded_at >= ? AND a.recorded_at < ?
            {store_filter_clause}
        ),
        filtered_data AS (
            SELECT *, (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
            FROM source_data
        )
        """
        return base_cte, params

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Lấy các chỉ số chính (KPIs) cho dashboard.

        Bao gồm tổng lượt vào, trung bình, giờ cao điểm, lượng khách hiện tại,
        cửa hàng đông nhất và tỷ lệ tăng trưởng so với kỳ trước.
        Xử lý các trường hợp không có dữ liệu để tránh lỗi.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        # Định dạng và đơn vị thời gian cho truy vấn
        # peak_time_format = {'day': '%H:00', 'week': '%Y-%m-%d', 'month': '%Y-%m-%d', 'year': 'Tháng %m'}.get(self.period, '%Y-%m-%d')
        peak_time_format = {'day': '%H:%M', 'week': '%d/%m', 'month': '%d/%m', 'year': 'Tháng %m'}.get(self.period, '%d/%m')
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        , period_summary AS (
            -- Tổng hợp dữ liệu theo từng kỳ (giống hệt biểu đồ)
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period,
                SUM(in_count) as total_in_per_period
            FROM filtered_data
            GROUP BY period
        )
        , peak_period AS (
            -- Tìm ra kỳ có tổng lượt vào cao nhất
            SELECT period FROM period_summary
            ORDER BY total_in_per_period DESC
            LIMIT 1
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (SELECT strftime(period + INTERVAL '{settings.WORKING_HOUR_START} hours', '{peak_time_format}') FROM peak_period) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """

        df, total_in_previous = await asyncio.gather(
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
        total_in_current = data.get('total_in', 0) or 0

        growth = 0.0
        if total_in_previous > 0:
            growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
        elif total_in_current > 0:
            growth = 100.0

        avg_val = data.get('average_in')
        data['average_in'] = 0 if pd.isna(avg_val) else int(round(avg_val))
        data['growth'] = growth
        if data.get('busiest_store'):
            data['busiest_store'] = data['busiest_store'].split(' (')[0]

        return data

    async def _get_previous_period_total_in(self) -> int:
        """ Tính tổng lượt khách của kỳ liền trước để so sánh tăng trưởng. """
        time_delta = self.end_date - self.start_date
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
                'start': self.start_date - relativedelta(months=1),
                'end': self.start_date - timedelta(days=1)
            },
            'year': {
                'start': self.start_date - relativedelta(years=1),
                'end': self.end_date - relativedelta(years=1)
            }
        }
        dates = period_logic.get(self.period)

        if not dates: return 0

        start_str, end_str = self._get_date_range_params(dates['start'], dates['end'])
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f'{base_cte} SELECT SUM(in_count) as total FROM filtered_data'
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        # Kiểm tra nếu DataFrame rỗng hoặc giá trị là NaN/None trước khi chuyển đổi
        if df.empty or pd.isna(df['total'].iloc[0]):
            return 0

        return int(df['total'].iloc[0])

    @staticmethod
    def get_all_stores() -> List[str]:
        """ Lấy danh sách duy nhất tất cả các cửa hàng có trong dữ liệu. """
        query = "SELECT DISTINCT store_name FROM dim_stores ORDER BY store_name"
        try:
            df = query_db_to_df(query)
            return df['store_name'].tolist()
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách cửa hàng: {e}", exc_info=True)
            return []

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """ Lấy dữ liệu chuỗi thời gian cho biểu đồ cột (column chart). """
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

        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        if time_unit == 'month': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """ Lấy dữ liệu phân bổ lượt khách theo từng cửa hàng cho biểu đồ tròn (donut chart). """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f"""
        {base_cte}
        SELECT store_name as x, SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        return df.to_dict(orient='records')

    @async_cache
    async def get_table_details(self) -> Dict[str, Any]:
        """ Lấy dữ liệu chi tiết cho bảng, giới hạn 31 dòng gần nhất. """
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
        data_query = f"{final_query_cte} SELECT * FROM query_result ORDER BY period DESC LIMIT 31"
        summary_query = f"{final_query_cte} SELECT SUM(total_in) as total_sum, AVG(total_in) as average_in FROM query_result"

        df, summary_df = await asyncio.gather(
            asyncio.to_thread(query_db_to_df, data_query, params=params),
            asyncio.to_thread(query_db_to_df, summary_query, params=params)
        )
        summary_data = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total_sum': 0, 'average_in': 0}
        
        total_sum = summary_data.get('total_sum', 0) or 0
        if total_sum > 0 and not df.empty:
            df['proportion_pct'] = (df['total_in'] / total_sum * 100)
        else:
            # Gán cột trống nếu không có dữ liệu để tránh lỗi
            df['proportion_pct'] = 0.0

        if not df.empty:
            # Bảng đang được sắp xếp theo thời gian GIẢM DẦN (DESC).
            # .diff(periods=-1) sẽ tính chênh lệch của dòng hiện tại so với dòng TIẾP THEO.
            # Vì dòng tiếp theo là kỳ cũ hơn, nên phép tính này chính là (hiện tại - quá khứ).
            df['proportion_change'] = df['proportion_pct'].diff(periods=-1).fillna(0)
        else:
            # Gán cột trống nếu không có dữ liệu
            df['proportion_change'] = 0.0

        return {
            'data': df.to_dict(orient='records'),
            'summary': summary_data
        }

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """ Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu. """
        query = f"SELECT MAX(recorded_at) as latest_time FROM fact_traffic"
        df = query_db_to_df(query)
        if not df.empty and pd.notna(df['latest_time'].iloc[0]):
            return df['latest_time'].iloc[0]

        return None

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """ Lấy các log lỗi gần nhất từ dữ liệu. """
        query = f"""
        SELECT a.log_id as id, b.store_name, a.logged_at as log_time, a.error_code, a.error_message
        FROM fact_errors AS a
        LEFT JOIN dim_stores AS b ON a.store_id = b.store_id
        ORDER BY a.logged_at DESC
        LIMIT ?
        """
        df = query_db_to_df(query, params=[limit])

        return df.to_dict(orient='records')

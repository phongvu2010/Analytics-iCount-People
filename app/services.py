import pandas as pd

from datetime import date, timedelta, datetime
from typing import List, Dict, Any, Optional
from dateutil.relativedelta import relativedelta

from .core.data_handler import query_parquet_as_dataframe
from .core.config import settings

class DashboardService:
    """
    Lớp chứa tất cả logic nghiệp vụ để lấy dữ liệu cho dashboard.
    Đã được cập nhật để sử dụng parameterized queries và tính toán growth động.
    """
    def __init__(self, period: str, start_date: date, end_date: date, store: str = 'all'):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

        # --- Logic xác định khoảng thời gian query dựa trên "ngày làm việc" ---
        # Dịch chuyển thời gian để khớp với logic "ngày làm việc" (09:00 - 02:00)
        # Ví dụ: chọn ngày 15/07, query sẽ lấy từ 09:00 ngày 15/07 đến 02:00 ngày 16/07
        start_dt = datetime.combine(start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)

        # End date cần cộng thêm 1 ngày và kết thúc vào giờ WORKING_HOUR_END
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)

        start_date_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # --- [FIX] Sửa lỗi SQL Injection bằng Parameterized Query ---
        # Tạo CTE (Common Table Expression) để tái sử dụng bộ dữ liệu đã lọc
        # Thêm một cột 'adjusted_time' để tính toán ngày làm việc chính xác
        self.params = [start_date_str, end_date_str]
        store_filter_clause = ""
        if store != 'all':
            store_filter_clause = "AND store_name = ?"
            self.params.append(store)

        # Xác định logic sẽ được áp dụng KHI vượt ngưỡng
        if settings.OUTLIER_SCALE_RATIO != 0:
            then_logic_in = f"CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)"
            then_logic_out = f"CAST(ROUND(out_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)"
        else:
            then_logic_in = "1"
            then_logic_out = "1"

        # Tạo CTE (Common Table Expression) để tái sử dụng bộ dữ liệu đã lọc
        self.base_cte = f"""
        WITH source_data AS (
            SELECT
                CAST(record_time AS TIMESTAMP) as record_time,
                store_name,
                CASE
                    WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in}
                    ELSE in_count
                END as in_count,
                CASE
                    WHEN out_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out}
                    ELSE out_count
                END as out_count
            FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
            WHERE record_time >= ? AND record_time < ?
            {store_filter_clause}
        ),
        filtered_data AS (
            SELECT *,
                -- Dịch chuyển thời gian lùi lại theo giờ bắt đầu để group by theo ngày làm việc
                (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
            FROM source_data
        )
        """

    def _get_previous_period_total_in(self) -> int:
        """
        [NEW] Hàm private để tính toán tổng lượt vào của kỳ trước đó.
        """
        # --- Tính toán khoảng thời gian của kỳ trước ---
        prev_start_date, prev_end_date = None, None

        if self.period == 'day':
            # Kỳ trước là ngày hôm qua
            time_delta = self.end_date - self.start_date
            prev_start_date = self.start_date - (time_delta + timedelta(days=1))
            prev_end_date = self.end_date - (time_delta + timedelta(days=1))
        elif self.period == 'week':
            # Kỳ trước là tuần trước đó
            prev_start_date = self.start_date - timedelta(weeks=1)
            prev_end_date = self.end_date - timedelta(weeks=1)
        elif self.period == 'month':
            # Kỳ trước là tháng trước đó
            prev_start_date = self.start_date - relativedelta(months=1)
            # End date của tháng trước cần tính toán cẩn thận để đảm bảo đúng số ngày
            last_day_of_prev_month = self.start_date - timedelta(days=1)
            prev_end_date = last_day_of_prev_month
        elif self.period == 'year':
            # Kỳ trước là năm trước đó
            prev_start_date = self.start_date - relativedelta(years=1)
            prev_end_date = self.end_date - relativedelta(years=1)

        if not prev_start_date or not prev_end_date:
            return 0

        # Áp dụng logic "ngày làm việc" cho kỳ trước
        prev_start_dt = datetime.combine(prev_start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)
        prev_end_dt = datetime.combine(prev_end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)

        prev_start_str = prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')
        prev_end_str = prev_end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Xây dựng câu query và tham số cho kỳ trước
        params = [prev_start_str, prev_end_str]
        store_filter_clause = ""
        if self.store != 'all':
            store_filter_clause = "AND store_name = ?"
            params.append(self.store)

        query = f"""
        SELECT SUM(
            CASE
                WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)
                ELSE in_count
            END
        ) as total
        FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
        WHERE record_time >= ? AND record_time < ?
        {store_filter_clause}
        """

        # Giả định query_parquet_as_dataframe đã được sửa để nhận tham số thứ hai
        # Ví dụ: def query_parquet_as_dataframe(query: str, params: list = None) -> pd.DataFrame:
        df = query_parquet_as_dataframe(query, params=params)

        if df.empty or df['total'].iloc[0] is None:
            return 0
        return int(df['total'].iloc[0])

    # def get_metrics(self) -> Dict[str, Any]:
    #     """
    #     Lấy các chỉ số chính (total, average, peak time...).
    #     """
    #     query = f"""
    #     {self.base_cte}
    #     SELECT
    #         SUM(in_count) as total_in,
    #         AVG(in_count) as average_in,
    #         strftime(arg_max(record_time, in_count), '%H:%M') as peak_time,
    #         (SUM(in_count) - SUM(out_count)) as current_occupancy,
    #         (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
    #     FROM filtered_data
    #     """
    #     # Mỗi hàm gọi query_parquet_as_dataframe phải truyền self.params
    #     df = query_parquet_as_dataframe(query, params=self.params)

    #     if df.empty or df['total_in'].iloc[0] is None:
    #         return {
    #             'total_in': 0,
    #             'average_in': 0,
    #             'peak_time': '--:--',
    #             'current_occupancy': 0,
    #             'busiest_store': 'N/A',
    #             'growth': 0.0
    #         }

    #     # --- [NEW] Tính toán tăng trưởng động ---
    #     total_in_current = df['total_in'].iloc[0]
    #     total_in_previous = self._get_previous_period_total_in()

    #     # Logic tính toán tăng trưởng (ví dụ: so với kỳ trước)
    #     growth = 0.0
    #     if total_in_previous > 0:
    #         growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
    #     elif total_in_current > 0:
    #         growth = 100.0 # Nếu kỳ trước không có dữ liệu nhưng kỳ này có -> Tăng trưởng 100%

    #     data = df.iloc[0].to_dict()
    #     data['average_in'] = round(data.get('average_in', 0), 1)
    #     data['growth'] = growth
    #     return data
    # def get_metrics(self) -> Dict[str, Any]:
    #     """
    #     Lấy các chỉ số chính (total, average, peak time...).
    #     """
    #     query = f"""
    #     {self.base_cte}
    #     SELECT
    #         SUM(in_count) as total_in,
    #         AVG(in_count) as average_in,
    #         strftime(arg_max(record_time, in_count), '%H:%M') as peak_time,
    #         (SUM(in_count) - SUM(out_count)) as current_occupancy,
    #         (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
    #     FROM filtered_data
    #     """
    #     df = query_parquet_as_dataframe(query, params=self.params)

    #     if df.empty or df['total_in'].iloc[0] is None:
    #         return {
    #             'total_in': 0,
    #             'average_in': 0,
    #             'peak_time': '--:--',
    #             'current_occupancy': 0,
    #             'busiest_store': 'N/A',
    #             'growth': 0.0
    #         }

    #     total_in_current = df['total_in'].iloc[0]
    #     total_in_previous = self._get_previous_period_total_in()

    #     growth = 0.0
    #     if total_in_previous > 0:
    #         growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
    #     elif total_in_current > 0:
    #         growth = 100.0

    #     data = df.iloc[0].to_dict()
    #     data['average_in'] = round(data.get('average_in', 0), 1)
    #     data['growth'] = growth
        
    #     # --- THÊM KHỐI CODE MỚI ĐỂ ĐỊNH DẠNG LẠI TÊN CỬA HÀNG ---
    #     busiest_store_raw = data.get('busiest_store')
    #     if busiest_store_raw:
    #         # 1. Thay thế "Floor " bằng "F_"
    #         formatted_store = busiest_store_raw.replace('Floor ', 'Floor ')
            
    #         # 2. Tìm và loại bỏ phần mô tả trong ngoặc đơn (nếu có)
    #         try:
    #             extra_info_pos = formatted_store.index(' (')
    #             formatted_store = formatted_store[:extra_info_pos]
    #         except ValueError:
    #             # Bỏ qua nếu không tìm thấy " ("
    #             pass
            
    #         data['busiest_store'] = formatted_store
    #     # --- KẾT THÚC KHỐI CODE MỚI ---
        
    #     return data
    # def get_metrics(self) -> Dict[str, Any]:
    #     """
    #     Lấy các chỉ số chính (total, average, peak time...).
    #     """
    #     # --- THÊM MỚI: Tự động xác định định dạng cho peak_time ---
    #     if self.period == 'day':
    #         peak_time_format = '%H:%M'
    #     elif self.period in ['week', 'month']:
    #         peak_time_format = '%d/%m'
    #     else:  # 'year' hoặc khoảng thời gian tùy chỉnh
    #         peak_time_format = '%m'
    #     # --- KẾT THÚC THÊM MỚI ---

    #     query = f"""
    #     {self.base_cte}
    #     SELECT
    #         SUM(in_count) as total_in,
    #         AVG(in_count) as average_in,
    #         -- THAY ĐỔI: Sử dụng định dạng đã xác định ở trên
    #         strftime(arg_max(record_time, in_count), '{peak_time_format}') as peak_time,
    #         (SUM(in_count) - SUM(out_count)) as current_occupancy,
    #         (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
    #     FROM filtered_data
    #     """
    #     df = query_parquet_as_dataframe(query, params=self.params)

    #     if df.empty or df['total_in'].iloc[0] is None:
    #         return {
    #             'total_in': 0, 'average_in': 0, 'peak_time': '--:--',
    #             'current_occupancy': 0, 'busiest_store': 'N/A', 'growth': 0.0
    #         }

    #     total_in_current = df['total_in'].iloc[0]
    #     total_in_previous = self._get_previous_period_total_in()

    #     growth = 0.0
    #     if total_in_previous > 0:
    #         growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
    #     elif total_in_current > 0:
    #         growth = 100.0

    #     data = df.iloc[0].to_dict()
    #     data['average_in'] = round(data.get('average_in', 0), 1)
    #     data['growth'] = growth
        
    #     busiest_store_raw = data.get('busiest_store')
    #     if busiest_store_raw:
    #         formatted_store = busiest_store_raw.replace('Floor ', 'Floor ')
    #         try:
    #             extra_info_pos = formatted_store.index(' (')
    #             formatted_store = formatted_store[:extra_info_pos]
    #         except ValueError:
    #             pass
    #         data['busiest_store'] = formatted_store
        
    #     return data
    def get_metrics(self) -> Dict[str, Any]:
        """
        Lấy các chỉ số chính (total, average, peak time...).
        """
        # --- THAY ĐỔI 1: Cập nhật lại định dạng cho "Cao điểm" ---
        if self.period == 'day':
            peak_time_format = '%H:%M'
        elif self.period in ['week', 'month']:
            # Hiển thị theo ngày/tháng
            peak_time_format = '%d/%m'
        else:  # 'year'
            # Hiển thị theo "Tháng X"
            peak_time_format = 'Tháng %m'
        
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {self.base_cte}
        , period_summary AS (
            SELECT SUM(in_count) as total_in_per_period
            FROM filtered_data
            GROUP BY date_trunc('{time_unit}', adjusted_time)
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (SELECT strftime(arg_max(record_time, in_count), '{peak_time_format}') FROM filtered_data) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """
        df = query_parquet_as_dataframe(query, params=self.params)

        if df.empty or df['total_in'].iloc[0] is None:
            return {
                'total_in': 0, 'average_in': 0, 'peak_time': '--:--',
                'current_occupancy': 0, 'busiest_store': 'N/A', 'growth': 0.0
            }

        total_in_current = df['total_in'].iloc[0]
        total_in_previous = self._get_previous_period_total_in()

        growth = 0.0
        if total_in_previous > 0:
            growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
        elif total_in_current > 0:
            growth = 100.0

        data = df.iloc[0].to_dict()
        
        # --- THAY ĐỔI 2: Làm tròn số TB lượt vào thành số nguyên ---
        data['average_in'] = int(round(data.get('average_in', 0) or 0))
        
        data['growth'] = growth
        
        busiest_store_raw = data.get('busiest_store')
        if busiest_store_raw:
            formatted_store = busiest_store_raw.replace('Floor ', 'Floor ')
            try:
                extra_info_pos = formatted_store.index(' (')
                formatted_store = formatted_store[:extra_info_pos]
            except ValueError:
                pass
            data['busiest_store'] = formatted_store
        
        return data

    # def get_trend_chart_data(self) -> List[Dict[str, Any]]:
    #     """
    #     Lấy dữ liệu cho biểu đồ đường, nhóm theo period được chọn.
    #     """
    #     time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

    #     # Sử dụng cột 'adjusted_time' để nhóm dữ liệu
    #     query = f"""
    #     {self.base_cte}
    #     SELECT
    #         date_trunc('{time_unit}', adjusted_time) as x,
    #         SUM(in_count) as y
    #     FROM filtered_data
    #     GROUP BY x
    #     ORDER BY x
    #     """
    #     df = query_parquet_as_dataframe(query, params=self.params)

    #     # Định dạng lại output cho phù hợp
    #     if time_unit == 'month':
    #         df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
    #     elif time_unit == 'day':
    #         df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
    #     else: # hour
    #          df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

    #     return df.to_dict(orient='records')
    def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """
        [ĐÃ CẬP NHẬT] Lấy dữ liệu cho biểu đồ đường, nhóm theo period được chọn và
        hiển thị đúng thời gian gốc.
        """
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        # Câu lệnh SQL được cập nhật để cộng lại số giờ đã dịch chuyển
        query = f"""
        {self.base_cte}
        SELECT
            -- THAY ĐỔI Ở ĐÂY: Cộng lại số giờ đã dịch chuyển để hiển thị thời gian gốc trên biểu đồ
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY x
        """
        df = query_parquet_as_dataframe(query, params=self.params)

        # Phần định dạng lại output cho phù hợp không cần thay đổi
        if time_unit == 'month':
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day':
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: # hour
            df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu cho biểu đồ donut (tỷ trọng theo cửa).
        """
        query = f"""
        {self.base_cte}
        SELECT
            store_name as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        df = query_parquet_as_dataframe(query, params=self.params)
        return df.to_dict(orient='records')

    # def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
    #     """
    #     [ĐÃ CẬP NHẬT] Lấy dữ liệu tổng hợp động theo period (ngày/tuần/tháng/năm),
    #     có phân trang và tính toán chênh lệch so với kỳ trước đó.
    #     """
    #     offset = (page - 1) * page_size

    #     # Tự động xác định đơn vị thời gian (time_unit) dựa trên bộ lọc period
    #     time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

    #     # Định dạng chuỗi ngày tháng cho cột hiển thị trên bảng
    #     date_format = {
    #         'hour': '%Y-%m-%d %H:00',
    #         'day': '%Y-%m-%d',
    #         'month': '%Y-%m',
    #     }.get(time_unit, '%Y-%m-%d')

    #     # CTE để nhóm theo time_unit, sau đó dùng window function LAG() để tính chênh lệch
    #     aggregation_query = f"""
    #     {self.base_cte}
    #     , period_summary AS (
    #         SELECT
    #             date_trunc('{time_unit}', adjusted_time) as period_start,
    #             SUM(in_count) as total_in
    #         FROM filtered_data
    #         GROUP BY period_start
    #     ),
    #     period_summary_with_lag AS (
    #         SELECT
    #             period_start,
    #             total_in,
    #             LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
    #         FROM period_summary
    #     )
    #     SELECT
    #         strftime(period_start, '{date_format}') as period,
    #         total_in,
    #         CASE
    #             WHEN previous_period_in = 0 THEN 0.0
    #             ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1)
    #         END as pct_change
    #     FROM period_summary_with_lag
    #     """

    #     # Lấy tổng số bản ghi để phân trang
    #     count_query = f"WITH query_result AS ({aggregation_query}) SELECT COUNT(*) as total FROM query_result"
    #     total_records_df = query_parquet_as_dataframe(count_query, params=self.params)
    #     total_records = total_records_df['total'].iloc[0] if not total_records_df.empty else 0

    #     # Lấy dữ liệu đã phân trang
    #     paginated_params = self.params + [page_size, offset]
    #     data_query = f"""
    #     WITH query_result AS ({aggregation_query})
    #     SELECT * FROM query_result
    #     ORDER BY period DESC
    #     LIMIT ? OFFSET ?
    #     """
    #     df = query_parquet_as_dataframe(data_query, params=paginated_params)

    #     return {
    #         'total_records': int(total_records),
    #         'page': page,
    #         'page_size': page_size,
    #         'data': df.to_dict(orient='records')
    #     }
    def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
        """
        [CẬP NHẬT] Lấy dữ liệu tổng hợp động theo period, trả về một trang duy nhất
        và kèm theo dữ liệu tổng kết.
        """
        offset = (page - 1) * page_size
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')
        date_format = {'hour': '%Y-%m-%d %H:00', 'day': '%Y-%m-%d', 'month': '%Y-%m',}.get(time_unit, '%Y-%m-%d')

        # aggregation_query = f"""
        # {self.base_cte}
        # , period_summary AS (...)
        # , period_summary_with_lag AS (...)
        # SELECT ... FROM period_summary_with_lag
        # """
        # # (Phần aggregation_query giữ nguyên như cũ, tôi rút gọn để bạn dễ nhìn)
        # aggregation_query = f"""
        # {self.base_cte}
        # , period_summary AS (
        #     SELECT date_trunc('{time_unit}', adjusted_time) as period_start, SUM(in_count) as total_in
        #     FROM filtered_data GROUP BY period_start
        # ),
        # period_summary_with_lag AS (
        #     SELECT period_start, total_in, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
        #     FROM period_summary
        # )
        # SELECT
        #     strftime(period_start, '{date_format}') as period, total_in,
        #     CASE WHEN previous_period_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1) END as pct_change
        # FROM period_summary_with_lag
        # """

        # CTE để nhóm theo time_unit, sau đó dùng window function LAG() để tính chênh lệch
        aggregation_query = f"""
        {self.base_cte}
        , period_summary AS (
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period_start,
                SUM(in_count) as total_in
            FROM filtered_data
            GROUP BY period_start
        ),
        period_summary_with_lag AS (
            SELECT
                period_start,
                total_in,
                LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
            FROM period_summary
        )
        SELECT
            -- THAY ĐỔI DUY NHẤT Ở DÒNG DƯỚI ĐÂY:
            -- Cộng lại số giờ đã dịch chuyển để hiển thị thời gian gốc
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE
                WHEN previous_period_in = 0 THEN 0.0
                ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1)
            END as pct_change
        FROM period_summary_with_lag
        """

        # --- THAY ĐỔI BẮT ĐẦU TỪ ĐÂY ---

        # Tạo một CTE chung để tái sử dụng
        final_query_cte = f"WITH query_result AS ({aggregation_query})"

        # 1. Lấy dữ liệu cho bảng (tối đa 31 dòng)
        paginated_params = self.params + [page_size, offset]
        data_query = f"""
        {final_query_cte}
        SELECT * FROM query_result
        ORDER BY period DESC
        LIMIT ? OFFSET ?
        """
        df = query_parquet_as_dataframe(data_query, params=paginated_params)

        # 2. Lấy dữ liệu tổng kết trên TOÀN BỘ khoảng thời gian (không phân trang)
        summary_query = f"""
        {final_query_cte}
        SELECT
            SUM(total_in) as total_sum,
            AVG(total_in) as average_in
        FROM query_result
        """
        summary_df = query_parquet_as_dataframe(summary_query, params=self.params)
        summary_data = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total_sum': 0, 'average_in': 0}

        # 3. Trả về cả dữ liệu bảng và dữ liệu tổng kết
        return {
            'total_records': len(df), # Total records giờ là số dòng thực tế trả về
            'page': page,
            'page_size': page_size,
            'data': df.to_dict(orient='records'),
            'summary': summary_data # Trả về summary
        }

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """Lấy ra timestamp của bản ghi gần nhất."""
        try:
            query = f"SELECT MAX(record_time) as latest_time FROM read_parquet('{settings.CROWD_COUNTS_PATH}')"
            df = query_parquet_as_dataframe(query)
            if not df.empty and pd.notna(df['latest_time'].iloc[0]):
                return df['latest_time'].iloc[0]
        except Exception:
            return None
        return None

    @staticmethod
    def get_all_stores() -> List[str]:
        """
        Lấy danh sách tất cả các cửa hàng.
        """
        query = f"SELECT DISTINCT store_name FROM read_parquet('{settings.CROWD_COUNTS_PATH}') ORDER BY store_name"
        df = query_parquet_as_dataframe(query)
        return df['store_name'].tolist()

    @staticmethod
    def get_error_logs(limit: int = 10) -> List[Dict[str, Any]]:
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

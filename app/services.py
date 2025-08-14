import logging
import pandas as pd

from datetime import datetime, timedelta
from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)

def get_store_names(db: DuckDBPyConnection) -> list[str]:
    """ Lấy danh sách các tên cửa hàng/vị trí độc nhất. """
    query = 'SELECT DISTINCT store_name FROM dim_stores ORDER BY store_name;'
    try:
        return db.execute(query).df()['store_name'].tolist()
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách cửa hàng: {e}", exc_info=True)
        return []

def _get_date_range_for_growth(start_date: datetime, end_date: datetime) -> tuple[datetime, datetime]:
    """ Tính toán khoảng thời gian của kỳ trước để so sánh tăng trưởng. """
    delta = end_date - start_date
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - delta
    return prev_start_date, prev_end_date

def get_dashboard_data(db: DuckDBPyConnection, start_date: str, end_date: str, period: str, store: str) -> dict:
    """ Hàm chính để truy vấn và tính toán tất cả dữ liệu thật cho dashboard. """
    start_date_dt = datetime.fromisoformat(start_date)
    end_date_dt = datetime.fromisoformat(end_date)

    # 1. Xây dựng câu lệnh WHERE và tham số
    params = [start_date, end_date]
    store_filter_clause = ''
    if store != 'all':
        store_filter_clause = 'AND s.store_name = ?'
        params.append(store)

    # 2. Truy vấn dữ liệu chính cho kỳ hiện tại
    query = f"""
        SELECT
            f.recorded_at,
            f.visitors_in,
            s.store_name
        FROM fact_traffic AS f
        JOIN dim_stores AS s ON f.store_id = s.store_id
        WHERE f.recorded_at::DATE BETWEEN ? AND ?
        {store_filter_clause}
    """
    try:
        main_df = db.execute(query, params).df()
        if not main_df.empty:
            main_df['recorded_at'] = pd.to_datetime(main_df['recorded_at'])
    except Exception as e:
        logger.error(f"Lỗi truy vấn dữ liệu chính: {e}", exc_info=True)
        main_df = pd.DataFrame(columns=['recorded_at', 'visitors_in', 'store_name'])

    # 3. Lấy 10 log lỗi gần nhất
    try:
        errors_df = db.execute("""
            SELECT l.logged_at, s.store_name, l.error_code, l.error_message
            FROM fact_errors l JOIN dim_stores s ON l.store_id = s.store_id
            ORDER BY l.logged_at DESC LIMIT 10
        """).df()
    except Exception:
        errors_df = pd.DataFrame()

    # 4. --- Bắt đầu tính toán các chỉ số (Metrics) ---
    if main_df.empty:
        # Trả về dữ liệu rỗng nếu không có bản ghi nào
        empty_chart = {'series': []}
        empty_table = {'data': [], 'summary': {}}

        return {
            'metrics': {'total_in': 0, 'average_in': 0, 'peak_time': None, 'busiest_store': None, 'growth': 0},
            'trend_chart': empty_chart,
            'store_comparison_chart': empty_chart,
            'table_data': empty_table,
            'error_logs': [],
            'latest_record_time': None
        }

    # 5. --- Bắt đầu tính toán các chỉ số (Metrics) ---
    # Tổng và trung bình
    total_in = int(main_df['visitors_in'].sum())
    num_days = (end_date_dt - start_date_dt).days + 1
    average_in = total_in / num_days if num_days > 0 else 0

    # Giờ cao điểm
    peak_hour = main_df.groupby(main_df['recorded_at'].dt.hour)['visitors_in'].sum().idxmax()
    peak_time_str = f"{peak_hour:02d}:00"

    # Vị trí đông nhất
    busiest_store = main_df.groupby('store_name')['visitors_in'].sum().idxmax()

    # Tính tăng trưởng
    prev_start, prev_end = _get_date_range_for_growth(start_date_dt, end_date_dt)
    growth_params = [prev_start.strftime('%Y-%m-%d'), prev_end.strftime('%Y-%m-%d')]
    if store != 'all':
        growth_params.append(store)

    prev_total_in = db.execute(f"SELECT SUM(visitors_in) FROM fact_traffic f JOIN dim_stores s ON f.store_id = s.store_id WHERE recorded_at::DATE BETWEEN ? AND ? {store_filter_clause}", growth_params).fetchone()[0]
    prev_total_in = prev_total_in or 0
    growth = ((total_in - prev_total_in) / prev_total_in * 100) if prev_total_in > 0 else 0

    metrics = {
        'total_in': total_in, 'average_in': round(average_in, 1),
        'peak_time': peak_time_str, 'busiest_store': busiest_store,
        'growth': round(growth, 1)
    }

    # 6. --- Chuẩn bị dữ liệu cho Biểu đồ và Bảng ---
    ## Biểu đồ xu hướng (Trend Chart)
    period_map = {'day': 'D', 'week': 'W-MON', 'month': 'M', 'year': 'Y'}
    trend_df = main_df.set_index('recorded_at').resample(period_map[period], label='left', closed='left')['visitors_in'].sum().reset_index()
    trend_df.rename(columns={'recorded_at': 'period', 'visitors_in': 'total_in'}, inplace=True)

    # Tính % thay đổi cho bảng
    trend_df['pct_change'] = trend_df['total_in'].pct_change().fillna(0) * 100

    # Định dạng lại ngày tháng
    if period == 'week':
        trend_df['period'] = trend_df['period'].dt.strftime('Tuần %W, %Y')
    elif period == 'month':
        trend_df['period'] = trend_df['period'].dt.strftime('%m-%Y')
    elif period == 'year':
        trend_df['period'] = trend_df['period'].dt.strftime('%Y')
    else: # day
        trend_df['period'] = trend_df['period'].dt.strftime('%d-%m-%Y')

    table_data = trend_df.to_dict('records')

    # Dữ liệu biểu đồ cần trục x là timestamp
    trend_df_chart = main_df.set_index('recorded_at').resample(period_map[period], label='left', closed='left')['visitors_in'].sum().reset_index()
    trend_chart_data = [{'x': row['recorded_at'].isoformat(), 'y': int(row['visitors_in'])} for _, row in trend_df_chart.iterrows()]

    store_df = main_df.groupby('store_name')['visitors_in'].sum().reset_index()
    store_chart_data = [{'x': row['store_name'], 'y': int(row['visitors_in'])} for _, row in store_df.iterrows()]

    # 7. --- Chuẩn bị dữ liệu Log lỗi và Dữ liệu mới nhất ---
    latest_record_time = main_df['recorded_at'].max().isoformat()
    error_logs = errors_df.rename(columns={'logged_at': 'log_time'}).to_dict('records')
    for log in error_logs:
        log['log_time'] = pd.to_datetime(log['log_time']).isoformat()

    # 8. Đóng gói tất cả dữ liệu trả về theo schema
    return {
        'metrics': metrics,
        'trend_chart': {'series': trend_chart_data},
        'store_comparison_chart': {'series': store_chart_data},
        'table_data': {
            'data': table_data,
            'summary': {'total_sum': total_in, 'average_in': round(average_in, 1)}
        },
        'error_logs': error_logs,
        'latest_record_time': latest_record_time
    }






    # # 5. --- Chuẩn bị dữ liệu cho Biểu đồ (Charts) ---
    # trend_df['recorded_at'] = trend_df['recorded_at'].dt.strftime('%Y-%m-%d')
    # trend_chart_data = [{'x': row['recorded_at'], 'y': int(row['visitors_in'])} for _, row in trend_df.iterrows()]

    # # Biểu đồ so sánh cửa hàng (Store Comparison)
    # store_df = main_df.groupby('store_name')['visitors_in'].sum().reset_index()
    # store_chart_data = [{'x': row['store_name'], 'y': int(row['visitors_in'])} for _, row in store_df.iterrows()]

    # # 6. --- Chuẩn bị dữ liệu cho Bảng tổng hợp (Table) ---
    # table_df = trend_df.copy()
    # table_df['total_in'] = table_df['visitors_in']
    # # Tính % thay đổi so với kỳ trước
    # table_df['pct_change'] = table_df['total_in'].pct_change().fillna(0) * 100
    # table_df['period'] = table_df['recorded_at'] # Đổi tên cột cho khớp schema
    # table_data = [
    #     {'period': row['period'], 'total_in': int(row['total_in']), 'pct_change': round(row['pct_change'], 1)}
    #     for _, row in table_df.iterrows()
    # ]

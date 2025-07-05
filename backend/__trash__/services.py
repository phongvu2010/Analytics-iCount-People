import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from . import models

def get_all_stores(db: Session):
    """ Lấy tất cả cửa hàng """
    return db.query(models.Store.tid, models.Store.name).all()

def get_recent_errors(db: Session, limit: int = 10):
    """ Lấy các lỗi gần nhất, join với tên cửa hàng """
    query = """
    SELECT TOP (:limit)
        e.LogTime as log_time,
        e.ErrorMessage as error_message,
        s.name as store_name
    FROM dbo.ErrLog e
    LEFT JOIN dbo.store s ON e.storeid = s.tid
    ORDER BY e.LogTime DESC
    """
    return db.execute(text(query), {'limit': limit}).fetchall()

def get_crowd_data(db: Session, start_date: datetime, end_date: datetime, store_id: int = None):
    """ Lấy dữ liệu ra vào thô và tổng hợp """
    base_query = """
    SELECT
        nc.recordtime,
        nc.in_num,
        nc.out_num,
        s.name as store_name
    FROM dbo.num_crowd nc
    JOIN dbo.store s ON nc.storeid = s.tid
    WHERE nc.recordtime BETWEEN :start_date AND :end_date
    """
    params = {"start_date": start_date, "end_date": end_date}

    if store_id:
        base_query += " AND nc.storeid = :store_id"
        params["store_id"] = store_id

    # Đọc dữ liệu vào Pandas DataFrame
    df = pd.read_sql(text(base_query), db.bind, params=params, parse_dates=['recordtime'])

    if df.empty:
        return {"raw_data": [], "summary_by_day": [], "summary_by_hour": []}

    # 1. Dữ liệu thô (raw_data)
    # Giới hạn 1000 dòng để tránh quá tải
    raw_data = df.sort_values(by='recordtime', ascending=False).head(1000).to_dict('records')

    # 2. Tổng hợp theo ngày (summary_by_day)
    df_daily = df.set_index('recordtime').groupby(pd.Grouper(freq='D'))[['in_num', 'out_num']].sum().reset_index()
    df_daily['date'] = df_daily['recordtime'].dt.strftime('%Y-%m-%d')
    summary_by_day = df_daily[['date', 'in_num', 'out_num']].to_dict('records')

    # 3. Tổng hợp theo giờ (summary_by_hour)
    df_hourly = df.set_index('recordtime').groupby(pd.Grouper(freq='H'))[['in_num', 'out_num']].sum().reset_index()
    df_hourly['hour'] = df_hourly['recordtime'].dt.strftime('%Y-%m-%d %H:00')
    summary_by_hour = df_hourly[['hour', 'in_num', 'out_num']].to_dict('records')

    return {
        "raw_data": raw_data,
        "summary_by_day": summary_by_day,
        "summary_by_hour": summary_by_hour
    }



















# import pandas as pd
# from typing import List, Dict

# def aggregate_crowd_data(data: List[Dict], period: str = 'daily') -> List[Dict]:
#     """ Nhóm dữ liệu đếm người theo ngày, tuần, tháng. """
#     if not data:
#         return []

#     df = pd.DataFrame([d.__dict__ for d in data])
#     if df.empty:
#         return []

#     # Tạo DataFrame từ dữ liệu truy vấn được
#     df = pd.DataFrame(data, columns=['recordtime', 'in_num', 'out_num'])
#     df['recordtime'] = pd.to_datetime(df['recordtime'])
#     df.set_index('recordtime', inplace = True)

#     rule_map = {'daily': 'D', 'weekly': 'W-MON', 'monthly': 'M'}
#     rule = rule_map.get(period, 'D')

#     # Nhóm theo chu kỳ và tính tổng số lượng vào/ra
#     agg_df = df[['in_num', 'out_num']].resample(rule).sum()

#     # Chuyển đổi kết quả về dạng JSON để trả về cho API
#     # Đổi tên cột `recordtime` thành `period` để dễ hiểu hơn ở frontend
#     agg_df.reset_index(inplace = True)
#     agg_df.rename(columns = {'recordtime': 'period'}, inplace = True)
#     agg_df['period'] = agg_df['period'].dt.strftime('%Y-%m-%d')

#     return agg_df.to_dict(orient = 'records')

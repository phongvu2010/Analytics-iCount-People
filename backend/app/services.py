# # app/services.py
# from sqlalchemy.orm import Session
# from sqlalchemy import text
# from . import models
# from datetime import datetime
# import pandas as pd

# def get_all_stores(db: Session):
#     """Lấy tất cả cửa hàng"""
#     return db.query(models.Store.tid, models.Store.name).all()

# def get_recent_errors(db: Session, limit: int = 10):
#     """Lấy các lỗi gần nhất, join với tên cửa hàng"""
#     query = """
#     SELECT TOP (:limit)
#         e.LogTime as log_time,
#         e.ErrorMessage as error_message,
#         s.name as store_name
#     FROM dbo.ErrLog e
#     LEFT JOIN dbo.store s ON e.storeid = s.tid
#     ORDER BY e.LogTime DESC
#     """
#     return db.execute(text(query), {'limit': limit}).fetchall()

# def get_crowd_data(db: Session, start_date: datetime, end_date: datetime, store_id: int = None):
#     """Lấy dữ liệu ra vào thô và tổng hợp"""
#     base_query = """
#     SELECT
#         nc.recordtime,
#         nc.in_num,
#         nc.out_num,
#         s.name as store_name
#     FROM dbo.num_crowd nc
#     JOIN dbo.store s ON nc.storeid = s.tid
#     WHERE nc.recordtime BETWEEN :start_date AND :end_date
#     """
#     params = {"start_date": start_date, "end_date": end_date}
    
#     if store_id:
#         base_query += " AND nc.storeid = :store_id"
#         params["store_id"] = store_id

#     # Đọc dữ liệu vào Pandas DataFrame
#     df = pd.read_sql(text(base_query), db.bind, params=params, parse_dates=['recordtime'])

#     if df.empty:
#         return {"raw_data": [], "summary_by_day": [], "summary_by_hour": []}

#     # 1. Dữ liệu thô (raw_data)
#     # Giới hạn 1000 dòng để tránh quá tải
#     raw_data = df.sort_values(by='recordtime', ascending=False).head(1000).to_dict('records')

#     # 2. Tổng hợp theo ngày (summary_by_day)
#     df_daily = df.set_index('recordtime').groupby(pd.Grouper(freq='D'))[['in_num', 'out_num']].sum().reset_index()
#     df_daily['date'] = df_daily['recordtime'].dt.strftime('%Y-%m-%d')
#     summary_by_day = df_daily[['date', 'in_num', 'out_num']].to_dict('records')
    
#     # 3. Tổng hợp theo giờ (summary_by_hour)
#     df_hourly = df.set_index('recordtime').groupby(pd.Grouper(freq='H'))[['in_num', 'out_num']].sum().reset_index()
#     df_hourly['hour'] = df_hourly['recordtime'].dt.strftime('%Y-%m-%d %H:00')
#     summary_by_hour = df_hourly[['hour', 'in_num', 'out_num']].to_dict('records')

#     return {
#         "raw_data": raw_data,
#         "summary_by_day": summary_by_day,
#         "summary_by_hour": summary_by_hour
#     }

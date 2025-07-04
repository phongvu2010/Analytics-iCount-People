from sqlalchemy.orm import Session
# from datetime import datetime
from . import models

# def get_crowd_data_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
#     """Lấy dữ liệu đếm người theo store và khoảng thời gian."""
#     return db.query(models.NumCrowd).filter(
#         models.NumCrowd.storeid == store_id,
#         models.NumCrowd.recordtime >= start_time,
#         models.NumCrowd.recordtime <= end_time
#     ).all()

# def get_error_logs_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
#     """Lấy log lỗi theo store và khoảng thời gian."""
#     return db.query(models.ErrLog).filter(
#         models.ErrLog.storeid == store_id,
#         models.ErrLog.LogTime >= start_time,
#         models.ErrLog.LogTime <= end_time
#     ).all()

def get_stores(db: Session):
    """ Lấy danh sách tất cả cửa hàng. """
    return db.query(models.Store).all()

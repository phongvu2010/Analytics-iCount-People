from datetime import datetime, timedelta
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from . import models

def get_stores(db: Session):
    """ Lấy danh sách tất cả các cửa hàng. """
    return db.query(models.Store).all()

def get_error_logs(db: Session, limit: int = 20):
    """ Lấy các cảnh báo lỗi gần nhất. """
    return db.query(models.ErrLog).order_by(desc(models.ErrLog.LogTime)).limit(limit).all()

def get_crowd_data_in_range(db: Session, start_date: datetime, end_date: datetime):
    """ Lấy dữ liệu lượt ra vào trong một khoảng thời gian. """
    return db.query(models.NumCrowd).filter(
        models.NumCrowd.recordtime >= start_date,
        models.NumCrowd.recordtime <= end_date
    ).all()

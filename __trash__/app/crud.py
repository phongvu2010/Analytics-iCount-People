from sqlalchemy.orm import Session
from datetime import datetime
from . import models

def get_crowd_data_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
    """ Lấy dữ liệu đếm người theo store và khoảng thời gian. """
    return db.query(models.NumCrowd).filter(
        models.NumCrowd.storeid == store_id,
        models.NumCrowd.recordtime >= start_time,
        models.NumCrowd.recordtime <= end_time
    ).all()

# def get_error_logs_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
#     """ Lấy log lỗi theo store và khoảng thời gian. """
#     return db.query(models.ErrLog).filter(
#         models.ErrLog.storeid == store_id,
#         models.ErrLog.LogTime >= start_time,
#         models.ErrLog.LogTime <= end_time
#     ).all()

def get_stores(db: Session):
    """ Lấy danh sách tất cả cửa hàng. """
    return db.query(models.Store).all()


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



# # === FILENAME: crud.py ===
# # Module chứa các hàm truy vấn và xử lý dữ liệu từ CSDL.

# from sqlalchemy.orm import Session
# from sqlalchemy import func, desc
# import models
# from datetime import datetime, timedelta

# def get_stores(db: Session):
#     """Lấy danh sách tất cả các cửa hàng."""
#     return db.query(models.Store).all()

# def get_error_logs(db: Session, limit: int = 20):
#     """Lấy các cảnh báo lỗi gần nhất."""
#     return db.query(models.ErrLog).order_by(desc(models.ErrLog.LogTime)).limit(limit).all()

# def get_crowd_data_in_range(db: Session, start_date: datetime, end_date: datetime):
#     """Lấy dữ liệu lượt ra vào trong một khoảng thời gian."""
#     return db.query(models.NumCrowd).filter(
#         models.NumCrowd.recordtime >= start_date,
#         models.NumCrowd.recordtime <= end_date
#     ).all()







from sqlalchemy.orm import Session
from datetime import datetime
from . import models

def get_crowd_data_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
    """ Lấy dữ liệu đếm người theo store và khoảng thời gian. """
    return db.query(models.NumCrowd).filter(
        models.NumCrowd.storeid == store_id,
        models.NumCrowd.recordtime >= start_time,
        models.NumCrowd.recordtime <= end_time
    ).all()

# def get_error_logs_by_filter(db: Session, store_id: int, start_time: datetime, end_time: datetime):
#     """ Lấy log lỗi theo store và khoảng thời gian. """
#     return db.query(models.ErrLog).filter(
#         models.ErrLog.storeid == store_id,
#         models.ErrLog.LogTime >= start_time,
#         models.ErrLog.LogTime <= end_time
#     ).all()

def get_stores(db: Session):
    """ Lấy danh sách tất cả cửa hàng. """
    return db.query(models.Store).all()

# from typing import List

# from ... import crud, schemas
# from ...core.database import get_db
# from ...services import analysis


# @router.get('/data/crowd', response_model = List[schemas.AggregatedCrowdData])
# def read_crowd_data(
#     store_id: int,
#     start_date: date = Query(..., description='Start date in YYYY-MM-DD format'),
#     end_date: date = Query(..., description='End date in YYYY-MM-DD format'),
#     period: str = Query('daily', enum=['daily', 'weekly', 'monthly']),
#     db: Session = Depends(get_db)
# ):
#     """ Lấy dữ liệu đếm người đã được tổng hợp theo ngày, tuần, hoặc tháng. """
#     start_time = datetime.combine(start_date, datetime.min.time())
#     end_time = datetime.combine(end_date, datetime.max.time())

#     raw_data = crud.get_crowd_data_by_filter(db, store_id, start_time, end_time)
#     aggregated_data = analysis.aggregate_crowd_data(raw_data, period)

#     return aggregated_data

# @router.get('/stores', response_model = List[schemas.Store])
# def get_all_stores(db: Session = Depends(get_db)):
#     """ Lấy danh sách tất cả cửa hàng. """
#     return crud.get_stores(db)



from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from .. import services
from ..core.database import get_db

router = APIRouter()

@router.get('/')
def read_crowd_data(
    # Thay đổi: Cho phép giá trị None bằng cách cung cấp default=None
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    store_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    # Chuyển đổi date thành datetime để query
    start_datetime = datetime.combine(start_date, datetime.min.time()) if start_date else None
    end_datetime = datetime.combine(end_date, datetime.min.time()) if end_date else None

    return services.get_crowd_data(db, start_datetime, end_datetime, store_id)




from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from .. import services
from ..core.database import get_db

router = APIRouter()

@router.get('/')
def read_crowd_data(
    # Thay đổi: Cho phép giá trị None bằng cách cung cấp default=None
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    store_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    # Chuyển đổi date thành datetime để query
    start_datetime = datetime.combine(start_date, datetime.min.time()) if start_date else None
    end_datetime = datetime.combine(end_date, datetime.min.time()) if end_date else None

    return services.get_crowd_data(db, start_datetime, end_datetime, store_id)











# from typing import List

# from ... import crud, schemas
# from ...core.database import get_db
# from ...services import analysis


# @router.get('/data/crowd', response_model = List[schemas.AggregatedCrowdData])
# def read_crowd_data(
#     store_id: int,
#     start_date: date = Query(..., description='Start date in YYYY-MM-DD format'),
#     end_date: date = Query(..., description='End date in YYYY-MM-DD format'),
#     period: str = Query('daily', enum=['daily', 'weekly', 'monthly']),
#     db: Session = Depends(get_db)
# ):
#     """ Lấy dữ liệu đếm người đã được tổng hợp theo ngày, tuần, hoặc tháng. """
#     start_time = datetime.combine(start_date, datetime.min.time())
#     end_time = datetime.combine(end_date, datetime.max.time())

#     raw_data = crud.get_crowd_data_by_filter(db, store_id, start_time, end_time)
#     aggregated_data = analysis.aggregate_crowd_data(raw_data, period)

#     return aggregated_data

# @router.get('/stores', response_model = List[schemas.Store])
# def get_all_stores(db: Session = Depends(get_db)):
#     """ Lấy danh sách tất cả cửa hàng. """
#     return crud.get_stores(db)

from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from .. import services
from ..database import get_db

router = APIRouter(prefix="/api/crowd", tags=["Crowd Data"])

@router.get("/")
def read_crowd_data(
    start_date: date = Query(default_factory=lambda: date.today()),
    end_date: date = Query(default_factory=lambda: date.today() + timedelta(days=1)),
    store_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    # Chuyển đổi date thành datetime để query
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.min.time())

    data = services.get_crowd_data(db, start_date=start_datetime, end_date=end_datetime, store_id=store_id)
    return data






# from datetime import datetime, date
# from fastapi import APIRouter, Depends, Query
# from sqlalchemy.orm import Session
# from typing import List

# from ... import crud, schemas
# from ...core.database import get_db
# from ...services import analysis

# router = APIRouter()

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

# from datetime import datetime, date
from fastapi import APIRouter, Depends # , Query
from sqlalchemy.orm import Session
from typing import List

from ... import crud, schemas # , services
from ...core.database import get_db

router = APIRouter()

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
#     aggregated_data = services.analysis.aggregate_crowd_data(raw_data, period)
#     return aggregated_data

@router.get('/stores', response_model = List[schemas.Store])
def get_all_stores(db: Session = Depends(get_db)):
    """ Lấy danh sách tất cả cửa hàng. """
    return crud.get_stores(db)

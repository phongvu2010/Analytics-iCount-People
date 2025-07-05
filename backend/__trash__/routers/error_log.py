from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from .. import services, models
from ..database import get_db

router = APIRouter(prefix="/api/errors", tags=["Errors"])

@router.get("/recent", response_model=List[models.ErrLogSchema])
def read_recent_errors(db: Session = Depends(get_db)):
    errors = services.get_recent_errors(db)
    return errors





# from fastapi import APIRouter, Depends, Query
# from sqlalchemy.orm import Session
# from typing import List
# from datetime import datetime, date

# from ... import crud, schemas
# from ...core.database import get_db
# from ..deps import get_current_user

# router = APIRouter()

# @router.get("/logs/errors", response_model=List[schemas.ErrLog])
# def read_error_logs(
#     store_id: int,
#     start_date: date = Query(..., description="Start date in YYYY-MM-DD format"),
#     end_date: date = Query(..., description="End date in YYYY-MM-DD format"),
#     db: Session = Depends(get_db),
#     current_user: str = Depends(get_current_user) # BẢO VỆ ENDPOINT
# ):
#     """
#     Lấy log lỗi. Yêu cầu phải đăng nhập (có token hợp lệ).
#     """
#     start_time = datetime.combine(start_date, datetime.min.time())
#     end_time = datetime.combine(end_date, datetime.max.time())
    
#     logs = crud.get_error_logs_by_filter(db, store_id, start_time, end_time)
#     return logs

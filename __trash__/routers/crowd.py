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

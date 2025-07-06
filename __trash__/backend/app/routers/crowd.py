from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from .. import services
from ..core.database import get_db

router = APIRouter()

@router.get('/')
def read_crowd_data(
    start_date: date = Query(default_factory=lambda: date.today()),
    end_date: date = Query(default_factory=lambda: date.today() + timedelta(days=1)),
    store_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    # Chuyển đổi date thành datetime để query
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.min.time())

    return services.get_crowd_data(db, start_datetime, end_datetime, store_id)

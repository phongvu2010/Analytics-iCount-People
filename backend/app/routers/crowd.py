# from fastapi import APIRouter, Depends, Query
# from sqlalchemy.orm import Session
# from datetime import datetime, date, timedelta
# from typing import Optional

# from .. import services
# from ..core.database import get_db

# router = APIRouter(prefix="/api/crowd", tags=["Crowd Data"])

# @router.get("/")
# def read_crowd_data(
#     start_date: date = Query(default_factory=lambda: date.today()),
#     end_date: date = Query(default_factory=lambda: date.today() + timedelta(days=1)),
#     store_id: Optional[int] = None,
#     db: Session = Depends(get_db)
# ):
#     # Chuyển đổi date thành datetime để query
#     start_datetime = datetime.combine(start_date, datetime.min.time())
#     end_datetime = datetime.combine(end_date, datetime.min.time())

#     data = services.get_crowd_data(db, start_date=start_datetime, end_date=end_datetime, store_id=store_id)
#     return data
